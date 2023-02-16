<?php

namespace TransCk\services;

use TransCk\db\MongoDb;
use TransCk\db\MysqlDb;

class CkService extends Base
{
    /**
     * 创建clickhouse建表
     */
    public function createTable()
    {
        //获取表结构
        $tableName = $this->m_table ?: $this->mo_table;
        $fieldMapping = (new SqlMappingService())->mappingField($tableName);
        $sqlField = '';
        foreach ($fieldMapping as $k => $item) {
            $sqlField .= PHP_EOL . $k . ' ' . $item . ',';
        }
        //生成clickhouse建表语句
        $tableName = $this->ck_table;
        $res = $this->conn->write("CREATE TABLE IF NOT EXISTS  {$tableName} (" . rtrim($sqlField, ",") . ")  engine = Memory;");
        if (!empty($res->error())) {
            Tools::writeLog('error.ck.create.table.info', json_encode($res->error()));
        } else {
            Tools::writeLog('success.ck.create.table.info', $tableName . ' 表添加成功！' . PHP_EOL . $this->conn->showCreateTable($tableName) . PHP_EOL);
        }
    }

    /**
     * 增量新增数据 即限制查询条件范围
     * 凌晨一点跑 昨天 的数据
     */
    public function incrementalDataByDay()
    {
        $timeStart = date("Y-m-d", strtotime("-1 day")) . ' 00:00:00';
        $timeEnd = date("Y-m-d") . ' 00:00:00 ';
        $where = [
            $this->time_key . ' > ' => $this->time_key_type == 'int' ? strtotime($timeStart) : $timeStart,
            $this->time_key . ' <= ' => $this->time_key_type == 'int' ? strtotime($timeEnd) : $timeEnd,
        ];
        $this->insertData($where, implode('|', $where));
    }

    /**
     *【按小时处理】
     * 增量新增数据 即限制查询条件范围
     */
    public function incrementalDataByHour()
    {
        $timeStart = date("Y-m-d H", strtotime("-1 hour")) . ':00:00';
        $timeEnd = date("Y-m-d H") . ':00:00';
        $where = [
            $this->time_key . ' > ' => $this->time_key_type == 'int' ? strtotime($timeStart) : $timeStart,
            $this->time_key . ' <= ' => $this->time_key_type == 'int' ? strtotime($timeEnd) : $timeEnd,
        ];
        $this->insertData($where, implode('|', $where));
    }

    /**
     *【自定义时间】
     * 增量新增数据 即限制查询条件范围
     * @param $params
     */
    public function incrementalDataBySelf($params)
    {
        $where = [
            $this->time_key . ' > ' => $this->time_key_type == 'int' ? strtotime($params['timeStart']) : $params['timeStart'],
            $this->time_key . ' <= ' => $this->time_key_type == 'int' ? strtotime($params['timeEnd']) : $params['timeEnd'],
        ];
        $this->insertData($where, implode('|', $where));
    }

    /**
     * 插入数据到clickhouse
     * @param string $where
     * @param string $logWhere
     */
    public function insertData($where = '', $logWhere = '')
    {
        $timeStart = Tools::getMillisecond();
        $tableName = $this->m_table ?: $this->mo_table;
        list($transData, $dirtyCount, $timeCheck) = !empty($this->m_table) ? $this->getMysqlData($where) : $this->getMongoData($where);
        $timeSearch = Tools::getMillisecond();
        $fields = $this->fields ?: array_keys((new SqlMappingService())->mappingField($tableName));
        if (!empty($transData)) {
            $stat = $this->conn->insert($this->ck_table,
                $transData,
                $fields
            );
            $timeEnd = Tools::getMillisecond();
            if (empty($stat->error())) {
                $log = $tableName . ' 表插入成功 ' . count($transData) . ' 条数据！其中剔除脏数据 ' . $dirtyCount
                    . ' 条 查询耗时' . (($timeSearch - $timeStart - $timeCheck) / 1000)
                    . '秒 校验耗时' . (($timeCheck) / 1000) . '秒 整体耗时' . (($timeEnd - $timeStart) / 1000) . '秒'
                    . PHP_EOL . $logWhere . PHP_EOL
                    . json_encode($stat->info(), JSON_PRETTY_PRINT);
                Tools::writeDayLog('success.ck.insert.info', $log);
            } else {
                Tools::writeDayLog('error.ck.insert.info', json_encode($stat->error(), JSON_PRETTY_PRINT));
            }
            unset($transData);
        } else {
            $timeEnd = Tools::getMillisecond();
            $log = $tableName . ' 表未插入数据！其中剔除脏数据 ' . $dirtyCount . ' 条 查询耗时'
                . (($timeSearch - $timeStart - $timeCheck) / 1000) . '秒 校验耗时'
                . (($timeCheck) / 1000) . '秒   整体耗时'
                . (($timeEnd - $timeStart) / 1000) . '秒' . PHP_EOL
                . $logWhere;
            Tools::writeDayLog('error.ck.insert.info', $log);
        }
    }

    /**
     * 获取数据
     * @param array $where
     * @return array
     */
    public function getMongoData($where = [])
    {
        $search = $partList = [];
        $batchNum = $this->single_search;
        $tableName = $this->mo_table;
        if (!empty($where)) {
            $whereNew = array_values($where);
            $search = [
                $this->time_key => [
                    'gt' => $whereNew[0]
                ],
                $this->time_key => [
                    'lte' => $whereNew[1]
                ]
            ];
        }

        $collection = (new MongoDb($this->mo_conf))->MoDB($this->mo_db, $tableName);
        $totalCount = $collection->countDocuments();
        $num = ceil($totalCount / $batchNum);

        $fieldMapping = (new SqlMappingService())->mappingField($tableName);

        $fields = $this->fields ? explode(',', $this->fields) : array_keys($fieldMapping);
        $fieldsNew = [];
        foreach ($fields as $field) {
            $fieldsNew[$field] = 1;
        }
        for ($i = 0; $i < $num; $i++) {
            $objData = $collection->find($search, [
                'projection' => $fieldsNew,
                'limit' => $batchNum,
                'skip' => $batchNum * $i
            ]);
            $resData = [];
            foreach ($objData as $document) {
                $documentInfo = json_decode(json_encode($document), true);
                $documentInfo['_id'] = $documentInfo['_id']['$oid'];
                $resData[] = $documentInfo;
            }
            $partList = array_merge($partList, $resData);
        }

        return $this->changeType($partList, $tableName, $where);
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
        $whereStr = $this->formatWhere($where);
        $totalCount = $mysqlDb->where($whereStr)->select("COUNT(*) as num");
        $totalCount = $totalCount[0]['num'];

        $num = ceil($totalCount / $batchNum);
        $fieldMapping = (new SqlMappingService())->mappingField($tableName);
        $fields = $this->fields ?: implode(',', array_keys($fieldMapping));

        for ($i = 0; $i < $num; $i++) {
            $offset = $batchNum * $i;
            $dataArr = $mysqlDb->where($where)->limit($batchNum)->offset($offset)->select($fields);
            $partList = array_merge($partList, $dataArr);
        }

        return $this->changeType($partList, $tableName, $where);
    }

    /**
     * 校验去重 & 格式转换
     * @param $partList
     * @param $tableName
     * @param $where
     * @return array
     */
    public function changeType($partList, $tableName, $where)
    {
        $timeStart = Tools::getMillisecond();
        //校验去重 兼容时间差导致的 脏数据
        list($partList, $dirtyCount) = $this->checkRepeat($partList, $where);
        $SqlMappingService = new SqlMappingService();

        $mappingChange = $SqlMappingService->mappingChange($tableName);
        $data = [];

        //强制转换数据类型
        foreach ($partList as $item) {
            $dataPart = [];
            foreach ($item as $key => $value) {
                $dataPart[] = $SqlMappingService->transMapping($mappingChange, $key, $value);
            }
            $data[] = $dataPart;
        }
        unset($partList);
        return [$data, $dirtyCount, (Tools::getMillisecond() - $timeStart)];
    }

    /**
     * 判断是否有重复数据 有则剔除重复数据
     * @param $data
     * @param string $where
     * @return array
     */
    public function checkRepeat($data, $where)
    {
        $primaryKey = $this->primary_key;
        $ckTable = $this->ck_table;
        $whereStr = $this->formatWhere($where);
        $ckData = $this->conn->select("SELECT {$primaryKey} FROM {$ckTable} WHERE {$whereStr} ");
        $ckIds = [];
        if (!empty($ckData->rawData()['data'])) {
            $ckIds = array_column($ckData->rawData()['data'], $primaryKey);
        }

        $dirtyCount = 0;
        if (!empty($ckIds)) {
            $newData = array_column($data, null, $primaryKey);
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
        return [$data, $dirtyCount];
    }

    public function formatWhere($where)
    {
        $where = $where ? $where : ' 1=1 ';
        if (is_array($where)) {
            $whereStr = '';
            foreach ($where as $key => $value) {
                $whereStr .= $key . " '" . $value . "' AND ";
            }
            $whereStr = trim($whereStr, 'AND ');
        } else {
            $whereStr = $where;
        }
        return $whereStr;
    }
}

