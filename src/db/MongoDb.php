<?php

namespace TransCk\db;

use MongoDB\Client;
use MongoDB\Collection;

class MongoDb
{
    public $config = [];
    public $db;

    /**
     * clickhouse
     * @param string $confDefault 配置
     */
    public function __construct($confDefault = 'mongo_conf_default')
    {
        //引入配置文件
        $config = require getenv("CONFIG_FILE_PATH");
        $this->config = $config[$confDefault];
        $this->db = new Client($this->config['hostname']);
    }

    /**
     * 获取DB
     * @param string $dbName 数据库名称
     * @param string $collectionName
     * @return Collection
     */
    public function MoDB($dbName = 'default', $collectionName = '')
    {
        return $this->db->selectCollection($dbName, $collectionName);
    }
}