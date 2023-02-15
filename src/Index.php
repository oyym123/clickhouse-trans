<?php

namespace TransCk;
require "../vendor/autoload.php";

use TransCk\services\CkService;

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
     * php index.php  -a=initData -b=20
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
        if ($date == 'month') { // month  day
            //按月跑数据
            $res = $this->monthList(strtotime($start), strtotime($end));
        } else {
            //按天跑数据
            $res = $this->dayList(strtotime($start), strtotime($end));
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

    /**
     * 生成从开始日期到结束日期的数组
     * @param $start int $start 开始时间戳
     * @param $end  int $end 结束时间戳
     * @return array|string
     */
    public function dayList($start, $end)
    {
        if (!is_numeric($start) || !is_numeric($end) || ($end <= $start)) return '';
        $date = [];
        while ($start <= $end) {
            $date[] = date('Y-m-d', $end);
            $end = $end - 3600 * 24;
        }
        return $date;
    }

    /**
     * 生成从开始月份到结束月份的月份数组
     * @param $start int $start 开始时间戳
     * @param $end  int $end 结束时间戳
     * @return array|string
     */
    public function monthList($start, $end)
    {
        if (!is_numeric($start) || !is_numeric($end) || ($end <= $start)) return '';
        $start = date('Y-m', $start);
        $end = date('Y-m', $end);
        //转为时间戳
        $start = strtotime($start . '-01');
        $end = strtotime($end . '-01');
        $i = 0;
        $d = array();
        while ($start <= $end) {
            //这里累加每个月的的总秒数 计算公式：上一月1号的时间戳秒数减去当前月的时间戳秒数
            $d[$i] = trim(date('Y-m-d', $start), ' ');
            $start += strtotime('+1 month', $start) - $start;
            $i++;
        }

        return $d;
    }

}

(new Index())->route();
