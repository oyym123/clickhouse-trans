<?php

namespace TransCk\db;

use ClickHouseDB\Client;

class ClickhouseDb
{
    public $config = [];
    public $db;

    /**
     * clickhouse
     * @param string $confDefault 配置
     */
    public function __construct($confDefault = 'ck_conf_default')
    {
        //引入配置文件
        $config = require getenv("CONFIG_FILE_PATH");
        $this->config = $config[$confDefault];
        $this->db = new Client($this->config);
    }

    /**
     * 获取DB
     * @param string $dbName 数据库名称
     * @return Client
     */
    public function CkDB($dbName = 'default')
    {
        $this->db->database($dbName);
        $this->db->setTimeout(1500);
        $this->db->setConnectTimeOut(500);
        return $this->db;
    }

}