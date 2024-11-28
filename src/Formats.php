<?php

namespace Topvisor\PDO_CH;

class Formats{

	// CSV
	const CSV = 'CSV';
	const CSVWithNames = 'CSVWithNames';
	// assoc with statistics
	const JSON = 'JSON';
	// assoc with statistics
	const JSONCompact = 'JSONCompact';
	// assoc
	const JSONEachRow = 'JSONEachRow';
	// Null
	const _NULL = 'Null';
	// Pretty text
	const Pretty = 'Pretty';
	const PrettyCompact = 'PrettyCompact';
	const PrettyCompactMonoBlock = 'PrettyCompactMonoBlock';
	const PrettySpace = 'PrettySpace';
	const PrettyNoEscapes = 'PrettyNoEscapes';
	const PrettyCompactNoEscapes = 'PrettyCompactNoEscapes';
	const PrettySpaceNoEscapes = 'PrettySpaceNoEscapes';
	// TabSeparated
	const TabSeparated = 'TabSeparated';
	const TabSeparatedRaw = 'TabSeparatedRaw';
	const TSVRaw = 'TSVRaw';
	const TabSeparatedWithNames = 'TabSeparatedWithNames';
	const TabSeparatedWithNamesAndTypes = 'TabSeparatedWithNamesAndTypes';
	// XML
	const XML = 'XML';
	// OBJECT
	const FETCH_OBJ = \PDO::FETCH_OBJ;
	const FETCH_ASSOC = \PDO::FETCH_ASSOC;
	const FETCH_NUM = \PDO::FETCH_NUM;
	const FETCH_COLUMN = \PDO::FETCH_COLUMN;
	const FETCH_KEY_PAIR = \PDO::FETCH_KEY_PAIR;

	static function checkForRead(string $format){
		if($format == self::FETCH_OBJ) return true;
		if($format == self::FETCH_ASSOC) return true;
		if($format == self::FETCH_NUM) return true;
		if($format == self::FETCH_COLUMN) return true;
		if($format == self::FETCH_KEY_PAIR) return true;
		if(defined("self::$format")) return true;

		return false;
	}

	static function checkForWrite(string $format){
		switch($format){
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

	static function prepareResult(string $result, ?string $format = NULL, ?int &$rows_before_limit_at_least = NULL, ?array &$statistics = NULL, ?array &$meta = NULL){
		$isAssocc = false;

		switch($format){
			case self::FETCH_ASSOC:
			case self::FETCH_NUM:
				$isAssocc = true;

			case self::FETCH_OBJ:
			case self::FETCH_COLUMN:
			case self::FETCH_KEY_PAIR:
				$result = json_decode($result, $isAssocc);
				if(!$result) return $result;

				if($isAssocc){
					$rows_before_limit_at_least = $result['rows_before_limit_at_least']??NULL;
					$statistics = $result['statistics']??NULL;
					$meta = $result['meta']??NULL;
				}else{
					$rows_before_limit_at_least = $result->rows_before_limit_at_least??NULL;
					$statistics = $result->statistics??NULL;
					$meta = $result->meta??NULL;
				}

				break;
		}

		switch($format){
			case self::FETCH_OBJ:
				$result = $result->data??NULL;

				break;

			case self::FETCH_ASSOC:
			case self::FETCH_NUM:
				$result = $result['data']??NULL;

				break;

			case self::FETCH_COLUMN:
				$result = array_column($result->data, 0);

				break;

			case self::FETCH_KEY_PAIR:
				$result = array_column($result->data, 1, 0);

				break;
		}

		return $result;
	}

}
