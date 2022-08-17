<?php

namespace app\services;

class SqlMappingService
{
    //简易映射 mysql => clickhouse
    const MAPPING = [
        'int' => 'Int32',
        'varchar' => 'String',
        'text' => 'String',
        'enum' => 'String',
        'decimal' => 'Decimal32(4)',
        'float' => 'Float32',
        'date' => 'Date',
        'datetime' => 'DateTime',
        'timestamp' => 'DateTime',
    ];

    public function mappingField($tableName)
    {
        $sql = self::DDL[$tableName];
        $sqlArr = explode(PHP_EOL, $sql);
        $keyMap = [];
        foreach ($sqlArr as $value) {
            $info = array_values(array_filter(explode(' ', $value)));
            if (isset($info[0]) && isset($info[1])) {
                $keyMap[str_replace('`', '', $info[0])] = $info[1];
            }
        }

        $ckField = [];

        //赋值映射
        foreach ($keyMap as $k => $item) {
            foreach (self::MAPPING as $key => $value) {
                if (strpos($item, $key) !== false) {
                    $ckField[$k] = $value;
                }
            }
        }
        return $ckField;
    }

    const DDL = [
        '`wf_202206_qiaqia`.`qq_access`' => "
                  `id` int(10) UNSIGNED NOT NULL COMMENT '表ID,AUTO_INCREMENT',
                  `user_id` int(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '用户ID',
                  `access_origin` varchar(8) NOT NULL DEFAULT '' COMMENT '访问方式来源',
                  `create_time` int(11) NOT NULL DEFAULT '0' COMMENT '数据创建时间',
                  ",
        '`wf_202206_qiaqia`.`qq_user`' => "
                  `id` int(10) UNSIGNED NOT NULL COMMENT '表ID,AUTO_INCREMENT',
                  `openid` varchar(128) NOT NULL DEFAULT '' COMMENT '微信openID',
                  `unionid` varchar(128) NOT NULL DEFAULT '' COMMENT '微信unionID',
                  `wx_nickname` varchar(64) DEFAULT NULL COMMENT '微信名称',
                  `wx_province` varchar(64) DEFAULT NULL COMMENT '微信用户的省份',
                  `wx_city` varchar(64) DEFAULT NULL COMMENT '微信用户的城市',
                  `wx_country` varchar(64) DEFAULT NULL COMMENT '微信用户的国家，如中国为CN',
                  `user_name` varchar(100) NOT NULL DEFAULT '' COMMENT '用户真实姓名',
                  `user_id_card` varchar(32) NOT NULL DEFAULT '' COMMENT '用户身份证号码',
                  `user_provice` varchar(20) DEFAULT NULL COMMENT '用户省份',
                  `user_city` varchar(20) DEFAULT NULL COMMENT '用户城市',
                  `create_time` int(11) NOT NULL DEFAULT '0' COMMENT '数据创建时间',
                  `register_time` int(11) NOT NULL DEFAULT '0' COMMENT '用户注册时间',                   
                  `register_origin` varchar(8) DEFAULT NULL COMMENT '注册来源',
                  `user_status` int(8) DEFAULT NULL COMMENT '用户状态 ， 1-正常，100-黑名单',
                  `login_origin` varchar(8) DEFAULT NULL COMMENT '访问来源',
                  `luck_num` int(11) NOT NULL DEFAULT '0' COMMENT '抽奖次数默认一人只能抽一次',
        ",
        '`wf_202206_qiaqia`.`qq_luck`' => "
                  `luck_code` varchar(32) NOT NULL DEFAULT '' COMMENT '抽奖码',
                  `luck_cannle` tinyint(4) NOT NULL DEFAULT '1' COMMENT '项目标志 1,2 ',
                  `user_id` int(11) NOT NULL DEFAULT '0' COMMENT '用户ID',
                  `luck_key` varchar(8) NOT NULL DEFAULT '' COMMENT '抽奖所得奖品标识',
                  `user_name` varchar(20) NOT NULL DEFAULT '' COMMENT '姓名',
                  `user_tel` varchar(12) NOT NULL DEFAULT '' COMMENT '电话',
                  `user_address` varchar(100) NOT NULL DEFAULT '' COMMENT '地址',
                  `luck_status` tinyint(4) NOT NULL DEFAULT '10' COMMENT '抽奖状态: 10-未生成二维码，20-有效未抽奖 , 0-未中奖，1-中奖未领取，2-中奖已领取，3-中奖已放弃',
                  `update_time` int(11) NOT NULL DEFAULT '0' COMMENT '数据更新时间',
                  `create_time` int(11) NOT NULL DEFAULT '0' COMMENT '数据创建时间',
        "
    ];
}

