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

## **配置项**
  * 在config.php 配置好默认的 mysql、clickhouse 数据库
  * 在 SqlMappingService.php 中添加需要同步到clickhouse的字段（直接复制建表sql）
  * 在 Base.php 中填写表的具体配置

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
  
  <img width="1436" alt="image" src="https://user-images.githubusercontent.com/20701868/185028830-ee7c64cb-dd4d-4ebd-9251-fdad5516915c.png">

  
  
  
