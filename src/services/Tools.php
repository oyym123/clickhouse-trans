<?php

namespace TransCk\services;
class Tools
{
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

    /**
     * 写日志方法 分日期
     * @param $file
     * @param $log
     */
    public static function writeDayLog($file, $log)
    {
        $varLogPath = getenv('LOG_DIR_PATH');
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
        $varLogPath = getenv('LOG_DIR_PATH');
        if (!is_dir($varLogPath)) {
            mkdir(iconv("UTF-8", "GBK", $varLogPath), 0777, true);
        }
        $log = date('Y-m-d H:i:s') . ' | ' . $log . PHP_EOL;
        $fileName = $varLogPath . $file . ".log";
        file_put_contents($fileName, $log, FILE_APPEND | LOCK_EX);
    }

    public static function getMillisecond()
    {
        list($t1, $t2) = explode(' ', microtime());
        return (float)sprintf('%.0f', (floatval($t1) + floatval($t2)) * 1000);
    }
}