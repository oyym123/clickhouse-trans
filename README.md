# clickhouse-trans
PHP  脚本  mysql 全量 &amp; 增量传输数据到 clickhouse  超轻量级代码 开箱即用

## **背景**
* 因业务需求 想用clickhouse做支撑 实现实时统计报表。调研了几个比较出名开源go写的 CDS同步 代码。
  都安装运行了，有界面化操作。但整体感觉 比较 “重”，不能随心所欲的玩耍，so 自己造个吧!

## **功能**
  * 可以添加多个 mysql 、clickhouse 配置
  * 可以自定义字段同步
  * 可以定时同步
  * 自动去重
  * 详细日志
  
## **安装**
```composer require oyym/clickhouse-trans```

## **配置项**
  * 在config.php 配置好默认的 mysql、clickhouse、mongo 数据库
  * 在Mapping.php 中添加需要同步到clickhouse的字段（直接复制mysql建表sql）

## **命令行**

  * 【-a 方法名   -b 类型id】 创建clickhouse表 
    * ``` php index.php  -a=createTable -b=10 ```

  * 【全量同步数据 数据不宜超过500万】大数据量请使用 incrementalDataBySelf() 方法分批导入     
    * ``` php index.php  -a=initData -b=10 ```

  * 【按小时 增量同步】每小时第5分钟跑  上个小时之间的数据 
    * ``` 5 * * * * php index.php  -a=incrementalDataByHour -b=10 ```

  * 【按天 增量同步】定时任务 每天凌晨2点同步数据  
     * ``` 0 2 * * * php index.php  -a=incrementalDataByHour -b=10 ```

  * 【-c 起始时间  -d 结束时间  -e (day:按天跑 month:按月跑)】新增指定日期间的数据 最小维度 【天】 
     * ``` php index.php -a=incrementalDataBySelf -b=10  -c=2021-11-01 -d=2022-07-01 -e=day ```
       * 按天跑的含义： 会获取 间隔时间内所有的天数，一天天的同步
       * 按月跑的含义： 会获取 间隔时间内所有的月份，一月月的同步
 
 ## **Laravel安装**
 ```composer require oyym/clickhouse-trans```
 ```php
    public function handle()
    {
        //入口函数需要设定 这三个文件的绝对路径
        putenv("CONFIG_FILE_PATH=" . __DIR__ . '/Config.php');          //日志文件地址
        putenv("MAPPING_FILE_PATH=" . __DIR__ . '/Mapping.php');        //映射文件地址
        putenv("LOG_DIR_PATH=" . storage_path() . '/logs/trans/');      //记录日志的目录地址

        //创建表
        (new CkService(Mapping::TYPE_PRICE_CHANGE_HISTORY))->createTable();

        //初始化数据
        (new CkService(Mapping::TYPE_PRICE_CHANGE_HISTORY))->insertData();

        //增量更新 每小时第5分钟跑 上个小时之间的数据  5 * * * * php artisan trans
        (new CkService(Mapping::TYPE_PRICE_CHANGE_HISTORY))->incrementalDataByHour();

        return true;
    }
```
运行 php artisan trans
 
  
  
