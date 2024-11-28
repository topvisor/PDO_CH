<?php

// Поддерживаемые FETCH преобразования:
//	PDO::FETCH_OBJ
//	PDO::FETCH_ASSOC
//	PDO::FETCH_NUM
//	PDO::FETCH_COLUMN
//	PDO::FETCH_KEY_PAIR
//	Большинство родных CH форматов https://clickhouse.yandex/docs/ru/formats/index.html

namespace Topvisor\PDO_CH;

class PDO_CH{

	protected $curlHundler = NULL;
	protected $error = NULL;
	protected $query = NULL;
	protected $resourceQuery = NULL;
	protected $options = NULL;
	protected $dsn = NULL;
	protected $rows_before_limit_at_least = NULL;
	protected $statistics = NULL;
	protected $meta = NULL;
	protected $username = NULL;
	protected $password = NULL;
	protected $curlOptions = [];

	function __construct(string $dsn, string $username, string $password, ?array $options = NULL, ?array $curlOptions = NULL){
		$this->dsn = $dsn;
		$this->options = $options;
		$this->username = $username;
		$this->password = $password;
		$this->genCurl();

		if($curlOptions) $this->curlOptions = $curlOptions;
	}

	function __destruct(){
		if($this->curlHundler) curl_close($this->curlHundler);
	}

	protected function genCurl(){
		$this->curlHundler = curl_init();
	}

	protected function setCurlOptions(string $query, array $queryOptions, array $curlOptions = []){
		$url = $this->genUrl($queryOptions);

		curl_reset($this->curlHundler);

		curl_setopt($this->curlHundler, CURLOPT_HEADER, 1);
		curl_setopt($this->curlHundler, CURLOPT_RETURNTRANSFER, 1);
		curl_setopt($this->curlHundler, CURLOPT_TIMEOUT, 60);
		curl_setopt($this->curlHundler, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
		curl_setopt($this->curlHundler, CURLOPT_SSL_VERIFYPEER, false);
		curl_setopt($this->curlHundler, CURLOPT_SSL_VERIFYHOST, false);

		if($queryOptions['max_execution_time']??NULL) curl_setopt($this->curlHundler, CURLOPT_TIMEOUT, (int)$queryOptions['max_execution_time']);

		$curlOptions = array_replace($this->curlOptions, $curlOptions);
		curl_setopt_array($this->curlHundler, $curlOptions);

		curl_setopt($this->curlHundler, CURLOPT_USERPWD, "$this->username:$this->password");

		if($queryOptions['enable_http_compression']??NULL){
			curl_setopt($this->curlHundler, CURLOPT_ENCODING, 'gzip');
			curl_setopt($this->curlHundler, CURLOPT_HTTPHEADER, ['Content-Encoding: gzip']);
		}

		if($this->resourceQuery){
			$url .= '&query='.urlencode($query);

//			// метод PUT не работает с версии  22.3
//			curl_setopt($this->curlHundler, CURLOPT_UPLOAD, true);
//			curl_setopt($this->curlHundler, CURLOPT_READFUNCTION, function($ch, $fd, $length){
//				return fread($this->resourceQuery, $length);
//			});

			curl_setopt($this->curlHundler, CURLOPT_POST, true);
			curl_setopt($this->curlHundler, CURLOPT_INFILE, $this->resourceQuery);
		}else{
			if($queryOptions['enable_http_compression']??NULL) $query = gzencode($query);

			curl_setopt($this->curlHundler, CURLOPT_POST, true);
			curl_setopt($this->curlHundler, CURLOPT_POSTFIELDS, $query);
		}

		curl_setopt($this->curlHundler, CURLOPT_URL, $url);
	}

	protected function genUrl(?array $queryOptions = NULL){
		$url = $this->dsn;
		if($queryOptions) $url .= '/?'.http_build_query($queryOptions);

		// длина параметров cURL
		$querySize = strlen($url) - strlen($this->dsn);
		if($querySize == 4078) $url .= ' ';

		return $url;
	}

	protected function throwError(string $message, int $code = 0){
		$this->error = new \stdClass();

//		$this->error->message = iconv('utf-8', 'utf-8//IGNORE', $message);
		$this->error->message = mb_convert_encoding($message, 'utf-8', 'utf-8');
		$this->error->code = $code;

		throw new \Exception($this->error->message, $this->error->code);
	}

	// внимание: при установки $async = true нет гарантии, что запрос будет выполнен, эмулируется через таймаут в 200 ms
	protected function call(?string $format = NULL, ?array $queryOptions = NULL, array $curlOptions = [], bool $async = false){
		$queryOptions = array_merge($this->options, (array)$queryOptions);

		$this->rows_before_limit_at_least = NULL;
		$this->statistics = NULL;
		$this->meta = NULL;

		$query = $this->query;
		$this->query = NULL; // освободить память

		if($format){
			if(!Formats::checkForRead($format)) $this->throwError("PDO_CH, config: Unknown read format $format");

			switch($format){
				case Formats::FETCH_OBJ:
				case Formats::FETCH_ASSOC:
					$query .= "\nFORMAT JSON";
					break;

				case Formats::FETCH_NUM:
				case Formats::FETCH_COLUMN:
				case Formats::FETCH_KEY_PAIR:
					$query .= "\nFORMAT JSONCompact";
					break;

				default:
					$query .= "\nFORMAT $format";
			}
		}

		if($async) $curlOptions[CURLOPT_TIMEOUT_MS] = 200;
		$this->setCurlOptions($query, $queryOptions, $curlOptions);
		$result = curl_exec($this->curlHundler);

		if(curl_errno($this->curlHundler)){
			if($async and curl_errno($this->curlHundler) == 28 and curl_getinfo($this->curlHundler)['size_upload']) return '';

			$this->throwError('Curl: '.curl_error($this->curlHundler), curl_errno($this->curlHundler));
		}

		$resultHeader = substr($result, 0, curl_getinfo($this->curlHundler, CURLINFO_HEADER_SIZE));
		$result = substr($result, curl_getinfo($this->curlHundler, CURLINFO_HEADER_SIZE));

		$statusCode = NULL;
		$statusesCodes = NULL;
		preg_match_all('/^HTTP\/\d\.?\d? (\d+) /m', $resultHeader, $statusesCodes);
		if($statusesCodes){
			$statusCode = $statusesCodes[1][count($statusesCodes[1]) - 1];
		}

		if($statusCode){
			if($statusCode != 200){
				if(preg_match('/Code: (\d+), (?:e\.code\(\) = (\d+), )?e\.displayText\(\) = (.+?), e\.what\(\) = (.+?)$/s', $result, $matches)){
					$code = $matches[1];
//					$code2 = $matches[2];
					$message = $matches[3];
//					$type = $matches[4];
				}else{
					$message = $result;
					$code = 0;
				}

				$this->throwError($message, $code);
			}
		}

		if($format){
			$result = Formats::prepareResult($result, $format, $this->rows_before_limit_at_least, $this->statistics, $this->meta);
		}

		if(is_null($result)) $this->throwError('Undefined error');

		return $result;
	}

	function query($query){
		$this->query = $query;

		return $this;
	}

	function fetchColumn($column_number = 0, ?array $options = NULL, array $curlOptions = []){
		$this->resourceQuery = NULL;

		$result = $this->call(Formats::FETCH_NUM, $options, $curlOptions);
		if(count($result) === 0)
			$result = false;
		else
			$result = $result[0][$column_number]??NULL;

		return $result;
	}

	// внимание, это SQL statement, не путать с PDOStatement
	// всегда возвращает первую строку результата
	function fetch($format = Formats::FETCH_ASSOC, ?array $options = NULL, array $curlOptions = []){
		$this->resourceQuery = NULL;
		if($format != Formats::FETCH_OBJ and $format != Formats::FETCH_ASSOC and $format != Formats::FETCH_NUM and $format != Formats::FETCH_COLUMN){
			$this->throwError('PDO_CH, fetch can only be used with FETCH_ASSOC or FETCH_NUM or FETCH_COLUMN');
		}

		$result = $this->fetchAll($format, $options, $curlOptions);

		$currentItem = 0;
		$result = $result[$currentItem]??false;

		return $result;
	}

	function fetchAll($format = Formats::FETCH_ASSOC, ?array $options = NULL, array $curlOptions = []){
		$this->resourceQuery = NULL;

		return $this->call($format, $options, $curlOptions);
	}

	function exec(string $query, ?array $options = NULL, array $curlOptions = [], bool $async = false){
		$this->query = $query;
		$this->resourceQuery = NULL;

		$result = $this->call(NULL, $options, $curlOptions, $async);

		if($result === '') return true;
	}

	function execWithResource(string $query, $resource, string $format, ?array $options = NULL, array $curlOptions = []){
		if(getType($resource) != 'resource') $this->throwError('$resource is not s valid resource');
		if(get_resource_type($resource) != 'stream') $this->throwError('$resource is not a valid stream resource');

		$this->query = $query;
		$this->resourceQuery = $resource;
		rewind($this->resourceQuery);

		$result = $this->call($format, $options, $curlOptions);

		if($result === '') return true;
	}

	function errorCode(){
		return $this->error->code??NULL;
	}

	function errorInfo(){
		return [
			'--',
			$this->error->code??NULL,
			$this->error->message??NULL
		];
	}

	function getRowsBeforeLimitAtLeast(){
		return $this->rows_before_limit_at_least;
	}

	function getStatistics(){
		return (array)$this->statistics;
	}

	function getMeta(){
		return (array)$this->meta;
	}

	function getCurlHundler(){
		return $this->curlHundler;
	}

}
