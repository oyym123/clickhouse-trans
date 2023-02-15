<?php

namespace TransCk\db;


class MysqlDb extends \PDO
{
    protected $tableName = "";//存储表名
    protected $sql = "";//存储最后执行的SQL语句
    protected $limit = "";//存储limit条件
    protected $offset = "";//存储offset条件
    protected $order = "";//存储order排序条件
    protected $field = "*";//存储要查询的字段
    protected $where = "";//存储where条件
    protected $allFields = [];//存储当前表的所有字段

    /**
     * 构造方法 初始化
     * @param string $tableName 要操作的表名
     */
    public function __construct($tableName = '', $confName = 'mysql_conf_default')
    {
        //引入配置文件
        $config = require getenv("CONFIG_FILE_PATH");
        $conf = $confName ? $config[$confName] : $config['mysql_conf_default'];
        //连接数据库
        parent::__construct('mysql:host=' . $conf['hostname'] . ';dbname=' . $conf['database'] .
            ';character=' . $conf['charset'] . ';port=' . $conf['port'], $conf['username'], $conf['password']);

        //存储表名
        $this->tableName = $tableName;

        //获取当前数据表中有哪些字段
        $this->getFields();
    }


    /**
     * 获取当前表的所有字段
     * @return array 成功则返回一维数组字段
     */
    public function getFields()
    {
        //查看当前表结构
        $sql = "desc {$this->tableName}";
        $res = $this->query($sql);//返回pdo对象
        //var_dump($res);
        if ($res) {
            $arr = $res->fetchAll(2);
            //var_dump($arr);
            //从二维数组中取出指定下标的列
            $this->allFields = array_column($arr, "Field");
            return $this->allFields;
        } else {
            die("表名错误");
        }
    }

    /**
     * @param $data
     * @return $this|int
     */
    public function add($data)
    {
        //判断是否是数组
        if (!is_array($data)) {
            return $this;
        }
        //判断是否全是非法字段
        if (empty($data)) {
            die("非法字段");
        }
        //过滤非法字段
        foreach ($data as $k => $v) {
            if (!in_array($k, $this->allFields)) {
                unset($data[$k]);
            }
        }
        //将数组中的键取出
        $keys = array_keys($data);
        //将数组中取出的键转为字符串拼接
        $key = implode(",", $keys);
        //将数组中的值转化为字符串拼接
        $value = implode("','", $data);
        //准备SQL语句
        $sql = "insert into {$this->tableName} ({$key}) values('{$value}')";
        $this->sql = $sql;
        //执行并发送SQL，返回受影响行数
        return (int)$this->exec($sql);
    }

    /**
     * @param string $id
     * @return int
     */
    public function delete($id = "")
    {
        //判断id是否存在
        if (empty($id)) {
            $where = $this->where;
        } else {
            $where = "where id={$id}";
        }
        $sql = "delete from {$this->tableName} {$where}";
        $this->sql = $sql;
        //执行并发送SQL,返回受影响行数
        return (int)$this->exec($sql);
    }

    /**
     * @param $data
     * @return $this|int
     */
    public function update($data)
    {
        //判断是否是数组
        if (!is_array($data)) {
            return $this;
        }
        //判断是否全是非法字段
        if (empty($data)) {
            die('全是非法字段');
        }
        $str = "";
        //过滤非法字段
        foreach ($data as $k => $v) {
            //字段为id时，判断id是否存在的
            if ($k == "id") {
                $this->where = "where id={$v}";
                unset($data[$k]);
                continue;
            }
            //若字段不为id，则过滤后再拼接成set字段
            if (in_array($k, $this->allFields)) {
                $str .= "{$k}='{$v}',";
            } else {
                unset($data[$k]);
            }
        }
        //判断是否传了条件
        if (empty($this->where)) {
            die('请传入修改条件');
        }
        //去除右边的,
        $str = rtrim($str, ',');
        $sql = "update {$this->tableName} set {$str} {$this->where}";
        //echo $sql;
        $this->sql = $sql;
        return (int)$this->exec($sql);
    }

    /**
     * @param string $field
     * @return array
     */
    public function select($field = '')
    {
        $this->field = $field ?: $this->field;
        $sql = "select {$this->field} from {$this->tableName} {$this->where} {$this->order} {$this->limit} {$this->offset}";
        $this->sql = $sql;
        //执行SQL,结果集是一个对象
        $res = $this->query($sql);
        //判断是否查询成功,
        if ($res) {
            //成功返回二维数组
            return $res->fetchAll(2);
        }
        //失败返回空数组
        return [];
    }

    /**
     * @param string $id
     * @return array
     */
    public function find($id = "")
    {
        //判断是否存在id
        if (empty($id)) {
            $where = $this->where;
        } else {
            $where = "where id={$id}";
        }
        $sql = "select {$this->field} from {$this->tableName} {$where} {$this->order} limit 1";
        $this->sql = $sql;
        //执行sql,结果集为对象
        $res = $this->query($sql);
        //判断是否查询成功
        if ($res) {
            //成功则返回一条数据(一维数组)
            $result = $res->fetchAll(2);
            return $result[0];
        }
        //失败返回空数组
        return [];
    }

    /**
     * 统计总数目
     * @return int 返回总数
     */
    public function count()
    {
        $sql = "select COUNT(*) as total from {$this->tableName} {$this->where} LIMIT 1";
        $this->sql = $sql;
        //执行SQL,结果集为对象
        $res = $this->query($sql);
        //处理结果集
        if ($res) {
            $result = $res->fetchAll(2);
            //var_dump($result);
            return $result[0]["total"];
        }
        return 0;
    }

    /**
     * 设置要查询的字段信息
     * @param string $field 要查询的字段
     * @return object 返回自己，保证连贯操作
     */
    public function field($field)
    {
        //判断字段是否存在
        if (empty($filed)) {
            return $this;
        }
        $this->field = $field;
        return $this;
    }

    /**
     * 获取最后执行的sql语句
     * @return string sql语句
     */
    public function _sql()
    {
        return $this->sql;
    }

    /**
     * where条件
     * @param string $where 要输入的where条件
     * @return object 返回自己，保证连贯操作
     */
    public function where($where)
    {
        $this->where = "where " . $where;
        return $this;
    }

    /**
     * order条件
     * @param string $order 要输入的order条件
     * @return object 返回自己，保证连贯操作
     */
    public function order($order)
    {
        $this->order = "order by " . $order;
        return $this;
    }

    /**
     * limit条件
     * @param string $limit 要输入的limit条件
     * @return object 返回自己，保证连贯操作
     */
    public function limit($limit)
    {
        $this->limit = "limit " . $limit;
        return $this;
    }

    /**
     * offset条件
     * @param string $offset 要输入的offset条件
     * @return object 返回自己，保证连贯操作
     */
    public function offset($offset)
    {
        $this->offset = "offset " . $offset;
        return $this;
    }
}

