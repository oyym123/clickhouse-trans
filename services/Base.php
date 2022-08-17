<?php

namespace app\services;


class Base
{
    //默认配置项
    public $m_conf = 'mysql_conf_default';    // 获取mysql 配置
    public $ck_conf = 'ck_conf_default';      // 获取clickhouse 配置
    public $ck_db = 'default';                // 获取clickhouse 数据库
    public $m_table = '';                     // 获取mysql 库名和表名
    public $ck_table = '';                    // 获取clickhouse表名
    public $primary_key = 'id';               // 获取主键名称
    public $time_key = 'create_time';         // 获取时间键的类型 【int | date】
    public $time_key_type = 'int';            // 获取增量更新依据的时间字段
    public $single_search = 10000;            // 获取单次搜索的数量
    public $fields = '';                      // 获取需要插入的字段 空 表示映射的所有字段

    public $type = 0;
    public $conn = '';

    public function __construct($type = 0)
    {
        if (!empty($type) && !in_array($type, array_keys(self::getTypeName()))) {
            exit('no this type ！');
        }

        include __DIR__ . '/../db/ClickhouseDb.php';

        //获取默认类型
        $this->type = $type;

        //加载配置
        $this->m_conf = $this->getConfig('m_conf') ?: $this->m_conf;
        $this->ck_conf = $this->getConfig('ck_conf') ?: $this->ck_conf;
        $this->ck_db = $this->getConfig('ck_db') ?: $this->ck_db;
        $this->m_table = $this->getConfig('m_table') ?: $this->m_table;
        $this->ck_table = $this->getConfig('ck_table') ?: $this->ck_table;
        $this->primary_key = $this->getConfig('primary_key') ?: $this->primary_key;
        $this->time_key = $this->getConfig('time_key') ?: $this->time_key;
        $this->time_key_type = $this->getConfig('time_key_type') ?: $this->time_key_type;
        $this->single_search = $this->getConfig('single_search') ?: $this->single_search;

        //连接clickhouse资源
        $this->conn = (new ClickhouseDb($this->ck_conf))->CkDB($this->ck_db);
    }

    /**********************************新增数据模型 START *****************************************************/
    const TYPE_QQ_ACCESS = 10;                                // 扫码表
    const TYPE_QQ_USER = 20;                                  // 用户表
    const TYPE_QQ_LUCK = 30;                                  // 二维码表

    public static function getTypeName($key = 'all')
    {
        $data = [
            self::TYPE_QQ_ACCESS => '扫码表',
            self::TYPE_QQ_USER => '用户表',
            self::TYPE_QQ_LUCK => '二维码表',
        ];
        return $key === 'all' ? $data : $data[$key];
    }


    /**
     * @param $field
     * @return string
     */
    public function getConfig($field)
    {
        //当要修改某些配置时，直接填上该字段，不填则使用默认值
        $data = [
            self::TYPE_QQ_ACCESS => [
                'm_table' => '`wf_202206_qiaqia`.`qq_access`',
                'ck_table' => 'qq_access',
            ],
            self::TYPE_QQ_USER => [
                'm_table' => '`wf_202206_qiaqia`.`qq_user`',
                'ck_table' => 'qq_user',
            ],
            self::TYPE_QQ_LUCK => [
                'm_table' => '`wf_202206_qiaqia`.`qq_luck`',
                'ck_table' => 'qq_luck',
                'primary_key' => 'luck_code',
            ],
        ];

        return $data[$this->type][$field] ?? '';
    }

    /**********************************新增数据模型 END *****************************************************/


    /**
     * 写日志方法 分日期
     * @param $file
     * @param $log
     */
    public static function writeDayLog($file, $log)
    {
        $varLogPath = __DIR__ . '/../logs/';
        if (!is_dir($varLogPath)) {
            mkdir(iconv("UTF-8", "GBK", $varLogPath), 0777, true);
        }
        $log = date('Y-m-d H:i:s') . ' | ' . $log . PHP_EOL;
        $fileName = $varLogPath . date("Ymd") . '.' . $file . ".log";
        file_put_contents($fileName, $log, FILE_APPEND | LOCK_EX);
    }

    /**
     * 写日志方法 不分日期
     * @param $file
     * @param $log
     */
    public static function writeLog($file, $log)
    {
        $varLogPath = __DIR__ . '/../logs/';
        if (!is_dir($varLogPath)) {
            mkdir(iconv("UTF-8", "GBK", $varLogPath), 0777, true);
        }
        $log = date('Y-m-d H:i:s') . ' | ' . $log . PHP_EOL;
        $fileName = $varLogPath . $file . ".log";
        file_put_contents($fileName, $log, FILE_APPEND | LOCK_EX);
    }

    public function getMillisecond()
    {
        list($t1, $t2) = explode(' ', microtime());
        return (float)sprintf('%.0f', (floatval($t1) + floatval($t2)) * 1000);
    }
}