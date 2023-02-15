<?php

namespace TransCk\services;

use TransCk\Mapping;

class SqlMappingService
{
    //简易映射 mysql => clickhouse
    const MAPPING = [
        'int' => 'Int32',
        'varchar' => 'String',
        'text' => 'String',
        'enum' => 'String',
        'decimal' => 'Decimal32(4)',
        'float' => 'Float32',
        'date' => 'Date',
        'datetime' => 'DateTime',
        'timestamp' => 'DateTime',
    ];

    /**
     * 获取数据结构，组装参数的转换类型
     * @param $tableName
     * @return array
     */
    public function mappingChange($tableName)
    {

        $model = new SqlMappingService();
        $fieldMapping = $model->mappingField($tableName);
        $typeMapping = ['Int32', 'String', 'Decimal32(4)', 'Float32', 'DateTime', 'Date'];
        $mappingRes = [];

        foreach ($fieldMapping as $k => $v) {
            foreach ($typeMapping as $value) {
                if ($value == $v) {
                    $mappingRes[$v][] = $k;
                }
            }
        }
        return $mappingRes;
    }

    public function mappingField($tableName)
    {
        $sql = Mapping::DDL[$tableName];
        $sqlArr = explode(PHP_EOL, $sql);
        $keyMap = [];
        foreach ($sqlArr as $value) {
            $info = array_values(array_filter(explode(' ', $value)));
            if (isset($info[0]) && isset($info[1])) {
                $keyMap[str_replace('`', '', $info[0])] = $info[1];
            }
        }

        $ckField = [];

        //赋值映射
        foreach ($keyMap as $k => $item) {
            foreach (self::MAPPING as $key => $value) {
                if (strpos($item, $key) !== false) {
                    $ckField[$k] = $value;
                }
            }
        }
        return $ckField;
    }

    /**
     * 组装转换类型后的数据
     * @param $mapping
     * @param $key
     * @param $value
     * @return float|int|string
     */
    public function transMapping($mapping, $key, $value)
    {
        if (in_array($key, $mapping['Int32'])) {
            return (int)$value;
        }

        if (isset($mapping['String']) && in_array($key, $mapping['String'])) {
            return (string)$value;
        }

        if (isset($mapping['Float32']) && in_array($key, $mapping['Float32'])) {
            return floatval($value);
        }

        if (isset($mapping['Decimal32(4)']) && in_array($key, $mapping['Decimal32(4)'])) {
            return floatval($value);
        }

        if (isset($mapping['DateTime']) && in_array($key, $mapping['DateTime']) && empty($value)) {
            return '1970-01-01 00:00:00';
        }

        if (isset($mapping['Date']) && in_array($key, $mapping['Date']) && empty($value)) {
            return '1970-01-01';
        }

        return $value;
    }
}

