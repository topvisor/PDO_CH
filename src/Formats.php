<?php

declare(strict_types=1);

namespace Topvisor\PDO_CH;

use PDO;

class Formats {
	// CSV
	public const string CSV = 'CSV';
	public const string CSVWithNames = 'CSVWithNames';
	// assoc with statistics
	public const string JSON = 'JSON';
	// assoc with statistics
	public const string JSONCompact = 'JSONCompact';
	// assoc
	public const string JSONEachRow = 'JSONEachRow';
	// Null
	public const string _NULL = 'Null';
	// Pretty text
	public const string Pretty = 'Pretty';
	public const string PrettyCompact = 'PrettyCompact';
	public const string PrettyCompactMonoBlock = 'PrettyCompactMonoBlock';
	public const string PrettySpace = 'PrettySpace';
	public const string PrettyNoEscapes = 'PrettyNoEscapes';
	public const string PrettyCompactNoEscapes = 'PrettyCompactNoEscapes';
	public const string PrettySpaceNoEscapes = 'PrettySpaceNoEscapes';
	// TabSeparated
	public const string TabSeparated = 'TabSeparated';
	public const string TabSeparatedRaw = 'TabSeparatedRaw';
	public const string TSVRaw = 'TSVRaw';
	public const string TabSeparatedWithNames = 'TabSeparatedWithNames';
	public const string TabSeparatedWithNamesAndTypes = 'TabSeparatedWithNamesAndTypes';
	// XML
	public const string XML = 'XML';
	// OBJECT
	public const int FETCH_OBJ = PDO::FETCH_OBJ;
	public const int FETCH_ASSOC = PDO::FETCH_ASSOC;
	public const int FETCH_NUM = PDO::FETCH_NUM;
	public const int FETCH_COLUMN = PDO::FETCH_COLUMN;
	public const int FETCH_KEY_PAIR = PDO::FETCH_KEY_PAIR;

	public static function checkForRead(int|string $format): bool {
		if (is_int($format)) {
			return in_array($format, [
				self::FETCH_OBJ,
				self::FETCH_ASSOC,
				self::FETCH_NUM,
				self::FETCH_COLUMN,
				self::FETCH_KEY_PAIR,
			], true);
		}

		if ($format === self::_NULL) return true;

		return defined(self::class . '::' . $format);
	}

	public static function checkForWrite(int|string $format): bool {
		switch ($format) {
			case self::CSV:
			case self::CSVWithNames:
			case self::JSONEachRow:
			case self::TabSeparated:
			case self::TabSeparatedRaw:
			case self::TSVRaw:
			case self::TabSeparatedWithNames:
			case self::TabSeparatedWithNamesAndTypes:
				return true;
		}

		return false;
	}

	public static function prepareResult(
		string $result,
		int|string|null $format = null,
		?int &$rows_before_limit_at_least = null,
		?array &$statistics = null,
		?array &$meta = null,
	): mixed {
		$isAssoc = false;

		switch ($format) {
			case self::FETCH_ASSOC:
			case self::FETCH_NUM:
				$isAssoc = true;

			// no break
			case self::FETCH_OBJ:
			case self::FETCH_COLUMN:
			case self::FETCH_KEY_PAIR:
				$result = json_decode($result, $isAssoc);
				if (!$result) return $result;

				if ($isAssoc) {
					$rows_before_limit_at_least = isset($result['rows_before_limit_at_least']) ? (int) $result['rows_before_limit_at_least'] : null;
					$statistics = (array) ($result['statistics'] ?? []);
					$meta = (array) ($result['meta'] ?? []);
				} else {
					$rows_before_limit_at_least = isset($result->rows_before_limit_at_least) ? (int) $result->rows_before_limit_at_least : null;
					$statistics = (array) ($result->statistics ?? []);
					$meta = (array) ($result->meta ?? []);
				}

				break;
		}

		switch ($format) {
			case self::FETCH_OBJ:
				$result = $result->data ?? null;

				break;

			case self::FETCH_ASSOC:
			case self::FETCH_NUM:
				$result = $result['data'] ?? null;

				break;

			case self::FETCH_COLUMN:
				$result = array_column((array) ($result->data ?? []), 0);

				break;

			case self::FETCH_KEY_PAIR:
				$result = array_column((array) ($result->data ?? []), 1, 0);

				break;
		}

		return $result;
	}

}
