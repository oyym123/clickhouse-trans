<?php

namespace TransCk;
require "../vendor/autoload.php";

use TransCk\services\CkService;
use TransCk\services\Tools;

class Index
{
    public $func = '';
    public $type = '';
    public $c = '';
    public $d = '';
    public $e = '';
    public $f = '';

    public function __construct()
    {
        //入口函数需要设定 这两个文件的绝对路径
        putenv("CONFIG_FILE_PATH=" . __DIR__ . '/Config.php');   //日志文件地址
        putenv("MAPPING_FILE_PATH=" . __DIR__ . '/Mapping.php'); //映射文件地址
        putenv("LOG_DIR_PATH=" . __DIR__ . '/../src/logs');      //记录日志的目录地址

        //以下仅为PHP cli命令行测试 ， 使用Laravel 、Yii2 等框架 可以使用其自带的console执行
        $arc = getopt('a:b:c:d:e:f:g');
        $this->func = $arc['a'] ?? '';
        $this->type = $arc['b'] ?? '';
        $this->c = $arc['c'] ?? '';
        $this->d = $arc['d'] ?? '';
        $this->e = $arc['e'] ?? '';
        $this->f = $arc['f'] ?? '';
    }

    public function route()
    {
        $func = $this->func;
        $this->$func();
    }

    /**
     * 创建clickhouse表
     * php index.php  -a=createTable -b=20
     */
    public function createTable()
    {
        (new CkService($this->type))->createTable();
    }

    /**
     * php index.php  -a=initData -b=50
     * 全量同步数据 【数据不宜超过200万】 大数据使用 incrementalDataBySelf() 方法分批导入
     * 初始化数据
     */
    public function initData()
    {
        set_time_limit(0);
        ini_set('memory_limit', '7048M');
        (new CkService($this->type))->insertData();
    }

    /**
     * 定时任务 每天凌晨2点同步数据  增量新增【按天】
     *  0 2 * * * php index.php  -a=incrementalDataByHour -b=20
     */
    public function incrementalDataByDay()
    {
        (new CkService($this->type))->incrementalDataByDay();
    }

    /**
     * 定时任务 增量新增【按小时】
     * 每小时第5分钟跑  上个小时之间的数据
     * 5 * * * * php index.php  -a=incrementalDataByHour -b=10
     */
    public function incrementalDataByHour()
    {
        (new CkService($this->type))->incrementalDataByHour();
    }

    /**
     * 自定义任务 增量新增
     * 新增指定日期间的数据 最小维度天
     * php index.php -a=incrementalDataBySelf -b=10  -c=2021-11-01 -d=2022-07-01 -e=day
     */
    public function incrementalDataBySelf()
    {
        set_time_limit(0);
        $start = $this->c;
        $end = $this->d;
        $date = $this->e;
        $tools = new Tools();
        if ($date == 'month') { // month  day
            //按月跑数据
            $res = $tools->monthList(strtotime($start), strtotime($end));
        } else {
            //按天跑数据
            $res = $tools->dayList(strtotime($start), strtotime($end));
            sort($res);
        }

        $ck = (new CkService($this->type));
        $total = count($res);
        foreach ($res as $k => $time) {
            $params['timeStart'] = $time . ' 00:00:00';
            if ($k < $total - 1) {
                $params['timeEnd'] = $res[$k + 1] . ' 00:00:00';
                $ck->incrementalDataBySelf($params);
            }
        }
    }
}

(new Index())->route();
