<?php

namespace TransCk\services;

use TransCk\Mapping;
use TransCk\db\ClickhouseDb;

class Base
{
    protected $conn = '';

    //默认配置项
    public $m_conf = 'mysql_conf_default';    // 获取mysql 配置
    public $ck_conf = 'ck_conf_default';      // 获取clickhouse 配置
    public $mo_conf = 'mongo_conf_default';   // 获取mongo 配置
    public $ck_db = 'default';                // 获取clickhouse 数据库
    public $mo_db = 'default';                // 获取mongo 数据库
    public $m_table = '';                     // 获取mysql 库名和表名
    public $ck_table = '';                    // 获取clickhouse表名
    public $mo_table = '';                    // 获取mongo表名
    public $primary_key = 'id';               // 获取主键名称
    public $time_key = 'create_time';         // 获取增量更新依据的时间字段
    public $time_key_type = 'int';            // 获取时间键的类型 【int 时间戳 | date 2022-02-01 00:00:00】
    public $single_search = 10000;            // 获取单次搜索的数量
    public $fields = '';                      // 获取需要插入的字段 空 表示映射的所有字段

    public function __construct($type = 0)
    {
        //引入映射文件
        require_once getenv("MAPPING_FILE_PATH");
        if (!empty($type) && !in_array($type, array_keys(Mapping::getTypeName()))) {
            exit('no this type ！');
        }

        $mapping = new Mapping();

        //获取默认类型
        $mapping->type = $type;

        //加载配置
        $this->m_conf = $mapping->getConfig('m_conf');
        $this->mo_conf = $mapping->getConfig('mo_conf');
        $this->ck_conf = $mapping->getConfig('ck_conf');
        $this->ck_db = $mapping->getConfig('ck_db');
        $this->mo_db = $mapping->getConfig('mo_db');
        $this->m_table = $mapping->getConfig('m_table');
        $this->mo_table = $mapping->getConfig('mo_table');
        $this->ck_table = $mapping->getConfig('ck_table');
        $this->primary_key = $mapping->getConfig('primary_key');
        $this->time_key = $mapping->getConfig('time_key');
        $this->time_key_type = $mapping->getConfig('time_key_type');
        $this->single_search = $mapping->getConfig('single_search');
        //连接clickhouse资源
        $this->conn = (new ClickhouseDb($this->ck_conf))->CkDB($this->ck_db);
    }

}