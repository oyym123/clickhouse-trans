<?php

namespace TransCk;

class Mapping
{
    //类型
    public $type = 0;

    /**********************************新增数据模型 START *****************************************************/
    const TYPE_QQ_ACCESS = 10;                                // 扫码表
    const TYPE_QQ_USER = 20;                                  // 用户表
    const TYPE_QQ_LUCK = 30;                                  // 二维码表
    const TYPE_PRICE_CHANGE_HISTORY = 50;                     // 历史价格变更记录

    public static function getTypeName($key = 'all')
    {
        $data = [
            self::TYPE_QQ_ACCESS => '扫码表',
            self::TYPE_QQ_USER => '用户表',
            self::TYPE_QQ_LUCK => '二维码表',
            self::TYPE_PRICE_CHANGE_HISTORY => '历史价格变更记录',
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
                'm_table' => '`wf`.`qq_access`',
                'ck_table' => 'qq_access',
            ],
            self::TYPE_QQ_USER => [
                'm_table' => '`wf`.`qq_user`',
                'ck_table' => 'default.qq_user',
            ],
            self::TYPE_QQ_LUCK => [
                'm_table' => '`wf`.`qq_luck`',
                'ck_table' => 'qq_luck',
                'primary_key' => 'luck_code',
            ],
            self::TYPE_PRICE_CHANGE_HISTORY => [
                'mo_db' => 'yibai_dcm_system',
                'mo_table' => 'change_product_price',
                'ck_table' => 'dcm_price_change_history',
                'time_key' => 'created_at',
                'primary_key' => '_id'
            ],
        ];
        return $data[$this->type][$field] ?? '';
    }

    /**********************************新增数据模型 END *****************************************************/


    /**********************************新增表格式映射 START **************************************************/
    const DDL = [
        '`wf`.`qq_access`' => "
                  `id` int(10) UNSIGNED NOT NULL COMMENT '表ID,AUTO_INCREMENT',
                  `user_id` int(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '用户ID',
                  `access_origin` varchar(8) NOT NULL DEFAULT '' COMMENT '访问方式来源',
                  `create_time` int(11) NOT NULL DEFAULT '0' COMMENT '数据创建时间',
                  ",
        '`wf`.`qq_user`' => "
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
        'change_product_price' => "
                  `_id` varchar(64) NOT NULL DEFAULT '',
                  `sku` varchar(255) NULL DEFAULT 0,
                  `last_price` decimal(10, 2) NOT NULL DEFAULT 0 COMMENT '旧的采购价',
                  `new_price` decimal(10, 2) NOT NULL COMMENT '最新采购价',
                  `effective_time` datetime(0) NULL COMMENT '生效时间',
                  `created_at` datetime(0) NULL COMMENT '创建时间',
        ",
    ];

    /**********************************新增表格式映射 END *****************************************************/
}