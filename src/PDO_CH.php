<?php

declare(strict_types=1);

// Поддерживаемые FETCH преобразования:
//	PDO::FETCH_OBJ
//	PDO::FETCH_ASSOC
//	PDO::FETCH_NUM
//	PDO::FETCH_COLUMN
//	PDO::FETCH_KEY_PAIR
//	Большинство родных CH форматов https://clickhouse.com/docs/interfaces/formats

namespace Topvisor\PDO_CH;

use CurlHandle;
use Exception;
use Throwable;

class PDO_CH {

	protected CurlHandle $curlHundler;
	protected ?Throwable $error = null;
	protected ?string $query = null;
	protected mixed $resourceQuery = null;
	protected array $options = [];
	protected ?int $rows_before_limit_at_least = null;
	protected ?array $statistics = null;
	protected ?array $meta = null;
	protected array $curlOptions = [];

	public function __construct(
		protected readonly string $dsn,
		protected readonly string $username,
		protected readonly string $password,
		?array $options = null,
		?array $curlOptions = null,
	) {
		$this->options = $options ?? [];
		$this->curlOptions = $curlOptions ?? [];
		$this->genCurl();
	}

	protected function genCurl(): void {
		$curlHundler = curl_init();
		if (!$curlHundler) {
			$this->throwError('Curl: failed to initialize');
		}

		$this->curlHundler = $curlHundler;
	}

	protected function setCurlOptions(string $query, array $queryOptions, array $curlOptions = []): void {
		$url = $this->genUrl($queryOptions);

		curl_reset($this->curlHundler);

		$options = [
			CURLOPT_HEADER => true,
			CURLOPT_RETURNTRANSFER => true,
			CURLOPT_TIMEOUT => 60,
			CURLOPT_IPRESOLVE => CURL_IPRESOLVE_V4,
			CURLOPT_SSL_VERIFYPEER => false,
			CURLOPT_SSL_VERIFYHOST => false,
		];

		if ($queryOptions['max_execution_time'] ?? null) {
			$options[CURLOPT_TIMEOUT] = (int) $queryOptions['max_execution_time'];
		}

		$options = array_replace($options, $this->curlOptions, $curlOptions);
		$options[CURLOPT_USERPWD] = "$this->username:$this->password";

		$compression = !empty($queryOptions['enable_http_compression']);
		$compressionCodec = null;

		if ($compression) {
			$options[CURLOPT_ENCODING] ??= 'gzip';

			foreach ($options[CURLOPT_HTTPHEADER] ?? [] as $header) {
				if (stripos($header, 'Content-Encoding:') === 0) {
					if (!preg_match('/Content-Encoding:\s*([\w-]+)/i', $header, $matches)) {
						$this->throwError('Curl: invalid Content-Encoding header');
					}

					$compressionCodec = strtolower($matches[1]);

					break;
				}
			}

			if ($compressionCodec === null) {
				$compressionCodec = 'gzip';
				$options[CURLOPT_HTTPHEADER][] = 'Content-Encoding: gzip';
			}
		}

		if ($this->resourceQuery !== null) {
			$url .= '&query=' . urlencode($query);

			// метод PUT не работает с версии 22.3
			$options[CURLOPT_POST] = true;
			$options[CURLOPT_INFILE] = $this->resourceQuery;
		} else {
			if ($compression) {
				switch ($compressionCodec) {
					case 'zstd':
						if (!function_exists('zstd_compress')) {
							$this->throwError('Curl: ext-zstd is required for zstd compression');
						}

						$compressedQuery = zstd_compress($query);
						break;

					case 'gzip':
						$compressedQuery = gzencode($query);

						break;

					default:
						$this->throwError('Curl: unsupported compression codec ' . $compressionCodec);
				}

				if ($compressedQuery === false) {
					$this->throwError('Curl: failed to compress query');
				}

				$query = $compressedQuery;
			}

			$options[CURLOPT_POST] = true;
			$options[CURLOPT_POSTFIELDS] = $query;
		}

		$options[CURLOPT_URL] = $url;

		curl_setopt_array($this->curlHundler, $options);
	}

	protected function genUrl(?array $queryOptions = null): string {
		$url = $this->dsn;

		if ($queryOptions) {
			$url .= (str_contains($url, '?') ? '&' : '/?') . http_build_query($queryOptions);
		} elseif (!str_contains($url, '?')) {
			$url .= '/?';
		}

		// длина параметров cURL
		$querySize = strlen($url) - strlen($this->dsn);
		if ($querySize === 4078) {
			$url .= ' ';
		}

		return $url;
	}

	protected function throwError(string $message, int $code = 0): never {
		$message = mb_convert_encoding($message, 'UTF-8', 'UTF-8');

		$this->error = new Exception($message, $code);

		throw $this->error;
	}

	// внимание: при установке $async = true нет гарантии, что запрос будет выполнен, эмулируется через таймаут в 200 ms
	protected function call(
		int|string|null $format = null,
		?array $queryOptions = null,
		array $curlOptions = [],
		bool $async = false,
	): mixed {
		$queryOptions = array_merge($this->options, $queryOptions ?? []);

		$this->rows_before_limit_at_least = null;
		$this->statistics = null;
		$this->meta = null;

		$query = $this->query ?? '';
		$this->query = null; // освободить память

		if ($format !== null) {
			if (!Formats::checkForRead($format)) {
				$this->throwError("PDO_CH, config: Unknown read format $format");
			}

			$query .= match ($format) {
				Formats::FETCH_OBJ,
				Formats::FETCH_ASSOC => "\nFORMAT JSON",
				Formats::FETCH_NUM,
				Formats::FETCH_COLUMN,
				Formats::FETCH_KEY_PAIR => "\nFORMAT JSONCompact",
				default => "\nFORMAT $format",
			};
		}

		if ($async) {
			$curlOptions[CURLOPT_TIMEOUT_MS] = 200;
		}

		$this->setCurlOptions($query, $queryOptions, $curlOptions);

		$result = curl_exec($this->curlHundler);

		$curlErrno = curl_errno($this->curlHundler);
		if ($curlErrno) {
			if ($async && $curlErrno === CURLE_OPERATION_TIMEDOUT && curl_getinfo($this->curlHundler, CURLINFO_SIZE_UPLOAD) > 0) {
				return '';
			}

			$this->throwError('Curl: ' . curl_error($this->curlHundler), $curlErrno);
		}

		if (!is_string($result)) {
			$this->throwError('Undefined error');
		}

		$headerSize = curl_getinfo($this->curlHundler, CURLINFO_HEADER_SIZE);
		$resultHeader = substr($result, 0, $headerSize);
		$result = substr($result, $headerSize);

		$statusCode = null;
		if (preg_match_all('/^HTTP\/\d\.?\d? (\d+) /m', $resultHeader, $statusesCodes) && $statusesCodes[1] !== []) {
			$statusCode = (int) array_last($statusesCodes[1]);
		}

		if ($statusCode !== null && $statusCode !== 200) {
			if (preg_match('/^\s*Code: (\d+)\.\s*(.+)$/s', $result, $matches)) {
				$code = (int) $matches[1];
				$message = $matches[2];
			} else {
				$message = $result;
				$code = 0;
			}

			$this->throwError($message, $code);
		}

		if ($format !== null) {
			$result = Formats::prepareResult($result, $format, $this->rows_before_limit_at_least, $this->statistics, $this->meta);
		}

		if ($result === null) {
			$this->throwError('Undefined error');
		}

		return $result;
	}

	public function query(string $query): static {
		$this->query = $query;

		return $this;
	}

	public function fetchColumn(int $column_number = 0, ?array $options = null, array $curlOptions = []): mixed {
		$this->resourceQuery = null;

		$result = $this->call(Formats::FETCH_NUM, $options, $curlOptions);
		if (!is_array($result) || $result === []) {
			return false;
		}

		$row = $result[0] ?? null;

		return is_array($row) ? ($row[$column_number] ?? null) : null;
	}

	// внимание, это SQL statement, не путать с PDOStatement
	// всегда возвращает первую строку результата
	public function fetch(int|string $format = Formats::FETCH_ASSOC, ?array $options = null, array $curlOptions = []): mixed {
		$this->resourceQuery = null;
		if (!in_array($format, [Formats::FETCH_OBJ, Formats::FETCH_ASSOC, Formats::FETCH_NUM, Formats::FETCH_COLUMN], true)) {
			$this->throwError('PDO_CH, fetch can only be used with FETCH_ASSOC or FETCH_NUM or FETCH_COLUMN');
		}

		$result = $this->fetchAll($format, $options, $curlOptions);

		return $result[0] ?? false;
	}

	public function fetchAll(int|string $format = Formats::FETCH_ASSOC, ?array $options = null, array $curlOptions = []): mixed {
		$this->resourceQuery = null;

		return $this->call($format, $options, $curlOptions);
	}

	public function exec(string $query, ?array $options = null, array $curlOptions = [], bool $async = false): ?bool {
		$this->query = $query;
		$this->resourceQuery = null;

		$result = $this->call(null, $options, $curlOptions, $async);

		if ($result === '') {
			return true;
		}

		return null;
	}

	public function execWithResource(string $query, mixed $resource, string $format, ?array $options = null, array $curlOptions = []): ?bool {
		if (!is_resource($resource)) {
			$this->throwError('$resource is not a valid resource');
		}

		if (get_resource_type($resource) !== 'stream') {
			$this->throwError('$resource is not a valid stream resource');
		}

		$this->query = $query;
		$this->resourceQuery = $resource;
		rewind($this->resourceQuery);

		$result = $this->call($format, $options, $curlOptions);

		if ($result === '') {
			return true;
		}

		return null;
	}

	public function errorCode(): ?int {
		return $this->error?->getCode();
	}

	public function errorInfo(): array {
		return [
			'--',
			$this->error?->getCode(),
			$this->error?->getMessage(),
		];
	}

	public function getRowsBeforeLimitAtLeast(): ?int {
		return $this->rows_before_limit_at_least;
	}

	public function getStatistics(): array {
		return $this->statistics ?? [];
	}

	public function getMeta(): array {
		return $this->meta ?? [];
	}

	public function getCurlHundler(): CurlHandle {
		return $this->curlHundler;
	}
}
