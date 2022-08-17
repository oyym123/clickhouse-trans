<?php

namespace app\services;

use app\db\MysqlDb;

include __DIR__ . '/../db/MysqlDb.php';
include __DIR__ . '/Base.php';
include __DIR__ . '/SqlMappingService.php';

class CkService extends Base
{

    /**
     * 创建clickhouse建表
     */
    public function createTable()
    {
        //获取mysql 表结构
        $fieldMapping = (new SqlMappingService())->mappingField($this->getConfig('m_table'));
        $sqlField = '';
        foreach ($fieldMapping as $k => $item) {
            $sqlField .= PHP_EOL . $k . ' ' . $item . ',';
        }

        //生成clickhouse建表语句
        $tableName = $this->ck_table;
        $res = $this->conn->write("CREATE TABLE IF NOT EXISTS  {$tableName} (" . rtrim($sqlField, ",") . ")  engine = Memory;");
        if (!empty($res->error())) {
            self::writeLog('error.ck.create.table.info', json_encode($res->error()));
        } else {
            self::writeLog('success.ck.create.table.info', $tableName . ' 表添加成功！' . PHP_EOL . $this->conn->showCreateTable($tableName) . PHP_EOL);
        }
    }

    /**
     * @param $type
     */

    /**
     * 增量新增数据 即限制查询条件范围
     * 凌晨一点跑 昨天 的数据
     */
    public function incrementalDataByDay()
    {
        $timeStart = date("Y-m-d", strtotime("-1 day")) . ' 00:00:00';
        $timeEnd = date("Y-m-d") . ' 00:00:00 ';
        if ($this->time_key_type == 'int') {
            $where =
                $this->time_key . ' > ' . strtotime($timeStart) . ' and ' .
                $this->time_key . ' <= ' . strtotime($timeEnd);
        } else {
            $where =
                $this->time_key . ' > ' . $timeStart . ' and ' .
                $this->time_key . ' <= ' . $timeEnd;
        }
        ini_set('memory_limit', '1048M');
        $this->insertData($where);
    }

    /**
     *【按小时处理】
     * 增量新增数据 即限制查询条件范围
     */
    public function incrementalDataByHour()
    {
        $timeStart = date("Y-m-d H", strtotime("-1 hour")) . ':00:00';
        $timeEnd = date("Y-m-d H") . ':00:00';
        if ($this->getConfig('time_key_type') == 'int') {
            $where =
                $this->time_key . ' > ' . strtotime($timeStart) . ' and ' .
                $this->time_key . ' <= ' . strtotime($timeEnd);
        } else {
            $where =
                $this->time_key . ' > ' . $timeStart . ' and ' .
                $this->time_key . ' <= ' . $timeEnd;
        }
        $this->insertData($where);
    }

    /**
     *【自定义时间】
     * 增量新增数据 即限制查询条件范围
     * @param $params
     */
    public function incrementalDataBySelf($params)
    {
        $timeStart = $params['timeStart'];
        $timeEnd = $params['timeEnd'];

        if ($this->time_key_type == 'int') {
            $where =
                $this->time_key . ' > ' . strtotime($timeStart) . ' and ' .
                $this->time_key . ' <= ' . strtotime($timeEnd);
            $logWhere = $this->time_key . ' >= ' . $timeStart . ' and ' .
                $this->time_key . ' <= ' . $timeEnd;
        } else {
            $where =
                $this->time_key . ' > ' . $timeStart . ' and ' .
                $this->time_key . ' <= ' . $timeEnd;
            $logWhere = $where;
        }

        ini_set('memory_limit', '1048M');
        $this->insertData($where, $logWhere);
    }


    public function insertData($where = '', $logWhere = '')
    {
        $timeStart = $this->getMillisecond();
        $tableName = $this->m_table;
        list($mysqlData, $dirtyCount) = $this->getMysqlData($where);
        $fields = $this->fields ?: array_keys((new SqlMappingService())->mappingField($tableName));

        if (!empty($mysqlData)) {
            $stat = $this->conn->insert($this->ck_table,
                $mysqlData,
                $fields
            );
            $timeEnd = $this->getMillisecond();
            if (empty($stat->error())) {
                $log = $tableName . ' 表插入成功 ' . count($mysqlData) . ' 条数据！其中剔除脏数据 ' . $dirtyCount . ' 条 整体耗时' . (($timeEnd - $timeStart) / 1000) . '秒' . PHP_EOL . $logWhere . PHP_EOL .
                    json_encode($stat->info(), JSON_PRETTY_PRINT);
                self::writeDayLog('success.ck.insert.info', $log);
            } else {
                self::writeDayLog('error.ck.insert.info', json_encode($stat->error(), JSON_PRETTY_PRINT));
            }
            unset($mysqlData);
        } else {
            $timeEnd = $this->getMillisecond();
            $log = $tableName . ' 表未插入数据！其中剔除脏数据 ' . $dirtyCount . ' 条 整体耗时' . (($timeEnd - $timeStart) / 1000) . '秒' . PHP_EOL . $logWhere;
            self::writeDayLog('error.ck.insert.info', $log);
        }
    }

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

    /**
     * 获取Mysql数据
     * @param string $where
     * @return array
     */
    public function getMysqlData($where = '')
    {
        $batchNum = $this->single_search;
        $partList = [];

        $tableName = $this->m_table;
        $mysqlDb = new MysqlDb($tableName, $this->m_conf);

        $where = $where ?: ' 1=1 ';

        $totalCount = $mysqlDb->where($where)->select("COUNT(*) as num");

        $totalCount = $totalCount[0]['num'];

        $num = ceil($totalCount / $batchNum);
        $fieldMapping = (new SqlMappingService())->mappingField($tableName);
        $fields = $this->fields ?: implode(',', array_keys($fieldMapping));


        for ($i = 0; $i < $num; $i++) {
            $offset = $batchNum * $i;
            $dataArr = $mysqlDb->where($where)->limit($batchNum)->offset($offset)->select($fields);
            $partList = array_merge($partList, $dataArr);
        }

        //校验去重 兼容时间差导致的 脏数据
        list($partList, $dirtyCount) = $this->checkRepeat($partList, $where);

        $mappingChange = $this->mappingChange($tableName);
        $data = [];

        //强制转换数据类型
        foreach ($partList as $item) {
            $dataPart = [];
            foreach ($item as $key => $value) {
                $dataPart[] = $this->transMapping($mappingChange, $key, $value);
            }
            $data[] = $dataPart;
        }
        unset($partList);
        return [$data, $dirtyCount];
    }

    /**
     * 判断是否有重复数据 有则剔除重复数据
     * @param $mysqlData
     * @param string $where
     * @return array
     */
    public function checkRepeat($mysqlData, $where = ' 1=1 ')
    {
        $primaryKey = $this->primary_key;
        $ckTable = $this->ck_table;
        $ckData = $this->conn->select("SELECT {$primaryKey} FROM {$ckTable} WHERE {$where} ");
        $ckIds = [];
        if (!empty($ckData->rawData()['data'])) {
            $ckIds = array_column($ckData->rawData()['data'], $primaryKey);
        }

        $dirtyCount = 0;
        if (!empty($ckIds)) {
            $newData = array_column($mysqlData, null, $primaryKey);
            $insertIds = array_flip(array_diff(array_keys($newData), $ckIds));
            $resData = [];
            foreach ($newData as $key => $datum) {
                if (isset($insertIds[$key])) {
                    $resData[] = $datum;
                } else {
                    $dirtyCount++;
                }
            }
            return [$resData, $dirtyCount];
        }
        return [$mysqlData, $dirtyCount];
    }
}

