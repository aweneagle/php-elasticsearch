<?php
namespace App\Lib;
/* 
 * ES, ElasticSearch 
 *
 * a easy tool for es operation
 *
 * 使用示例：
 *  查询 商店中作者 "king" 和 "bible" 两人价格为 50~100的
 *  含有"Elasticsearch" 的书
 *
 * $es = new ES;
 * $es->index("store")
 *    ->type("books")
 *    ->must(function(ES $es) {
 *      $es->where("author", "in", ["king", "bible"]);
 *      $es->must(function(ES $es) {
 *          $es->where("price", "<=", 100);
 *          $es->where("price", ">=", 50);
 *      });
 *    })
 *    ->should(function(ES $es) {
 *      $es->match("title", "Elasticsearch", 2);    //标题最重要
 *      $es->match("subtitle", "Elasticsearch");    //子标题其次
 *      $es->should(function(ES $es) {
 *          $es->match("desc", "Elasticsearch");    //描述 和 内容没那么重要
 *          $es->match("content", "Elasticsearch");
 *      });
 *    })
 *    ->sort_score()             //按照内容重要性排序
 *    ->sort('date', 'desc')    //再按发布时间排序
 *    ->search();               //查询数据
 *
 *
 */

class ES
{
    const INDEX_EXISTS = 1;
    const INDEX_NOT_EXISTS = 0;
    const INDEX_UNKNOWN = -1;

    private static $meta_fields = [
        "_score", "_index", "_type", "_id", "_source"
    ];

    public $host = '127.0.0.1';
    public $port = 9200;
    public $timeout_ms = 0;    //curl函数超时时间，单位是毫秒, <= 0 表示不超时
    public $conntimeout_ms = 0;    //连接超时时间，单位是毫秒, <= 0 表示不超时

    private $error = null;

    private $index = null;
    private $type = null;

    private $size = 0;  //分页大小, 默认不设置
    private $from = 0;   //默认从第一条开始

    private $_source = [];    //需要选取的字段
    private $fields_to_user = [];  //需要返回给调用者的字段
    private $meta = [];       //需要选取的元字段, 例如 _id, _score, _source 等
    private $no_select = true;

    private $query = [];
    private $sort = [];
    private $filter = [];
    private $cache = [];

    //标记当前的 must/should/must_not 所处的上下文
    //null 为初始化, "*" 为未知, "filter", "query" 分别对应过滤和查询
    private $context = null;   

    private $curr_bool = [];

    private $bool_stack = [];

    /* ES请求的http返回码 */
    private $status = null;
    private $nested_path = [];
    /* 这些key不返回给user */


    public function __construct($conf = null)
    {
        if ($conf) {
            $this->host = $conf['host'];
            $this->port = $conf['port'];
            $this->index = $conf['index'];
        }
    }

    /* 
     * get_index_name() 获取索引名
     */
    public function get_index_name()
    {
        return $this->index;
    }

    /*
     * index() 选择索引
     */
    public function index($index_name = null)
    {
        $this->clean_query();
        $this->index = $index_name;
        return $this;
    }

    /*
     * size() 设置分页大小
     */
    public function size($size)
    {
        $this->size = intval($size);
        return $this;
    }

    /*
     * from() 设置偏移量
     */
    public function from($offset)
    {
        $this->from = intval($offset);
        return $this;
    }

    /*
     * type() 选择type
     */
    public function type($type_name = null)
    {
        $this->clean_query();
        $this->type = $type_name;
        return $this;
    }

    /*
     * error() 查看错误信息
     */
    public function error()
    {
        return $this->error;
    }

    /*
     * select() 指定哪些字段需要返回, 相当于 select_multi([...])
     *
     * @param  $fields 支持'as'功能，例如： ["cpid", "cpid as sceneCode"]
     */
    public function select()
    {
        return $this->select_multi(func_get_args());
    }

    /*
     * select_multi() 指定哪些字段需要返回
     *
     * @param  $fields 支持'as'功能，例如： ["cpid", "cpid as sceneCode"]
     */
    public function select_multi(array $fields)
    {
        if (!empty($fields)) {
            $this->no_select = false;
        }
        foreach ($fields as $f) {
            if (is_string($f)) {
                if (!preg_match('/^([\w_.-]+)\s+as\s+([^\s]+)$/', $f, $farr)) {
                    $field = $f;
                    $alias = $f;
                } else {
                    $field = $farr[1];
                    $alias = $farr[2];
                }

                if (in_array($field, self::$meta_fields)) {
                    $this->meta[$field][] = $alias;
                } else {
                    $this->_source[$field][] = $alias;
                    $this->fields_to_user[$field] = 1;
                }
            }
        }
        return $this;
    }

    /* 
     * index_exists() 判断是否存在该index
     *
     * @return  1, 存在;  0 不存在; -1 请求失败, 调用 error() 查看错误信息
     */
    public function index_exists()
    {
        $res = $this->mapping();
        if ($res === false) {
            if ($this->status != null) {
                return 0;
            } else {
                return -1;
            }
        }
        return 1;
    }

    /*
     * create()  创建索引
     */
    public function create($mappings = []) {
        $url = $this->build_url("");
        $res = $this->curl($url, "PUT");
        if ($res === false) {
            return false;
        }
        $res = json_decode($res, true);
        if (isset($res['error'])) {
            $this->set_error(json_encode($res));
            $this->status = $res['status'];
            return false;
        } else {
            $this->status = 200;
        }

        return $res;
    }

    /*
     * drop() 删除整个索引
     */
    public function drop_index()
    {
        $url = $this->build_url("");
        $res = $this->curl($url, "DELETE");
        if ($res === false) {
            return false;
        }
        $res = json_decode($res, true);
        if (isset($res['error'])) {
            $this->set_error(json_encode($res));
            $this->status = $res['status'];
            return false;
        } else {
            $this->status = 200;
        }
        return $res;
    }

    /*
     * mapping() 获取/设置 mapping
     *
     * @param   $mapping, 需要设置的mapping
     * @return  $mapping
     */
    public function mapping(array $mappings = [])
    {
        $url = $this->build_url("_mapping");
        if ($mappings) {
            foreach ($mappings as $type => $map) {
                $properties = [];
                foreach ($map as $field => $data_type) {
                    $this->put_deep_properties(
                        $properties,
                        explode('.', $field), 
                        $this->get_type_mapping($data_type)
                    );
                }
                $this->curl($url . "/" . $type, "PUT", json_encode($properties));
            }
        }
        $res = $this->curl($url, "GET");
        if ($res === false) {
            return false;
        }
        $res = json_decode($res, true);
        if (isset($res['status'])) {
            $this->status = $res['status'];
            $this->set_error(json_encode($res));
            return false;
        } else {
            $this->status = 200;
        }
        return $res;
    }

    /*
     * alias() 创建别名
     *
     * @param   $alias, string, 别名
     */
    public function alias($alias)
    {
        $url = $this->build_url("_alias") . "/" . $alias;
        $res = $this->curl($url, "PUT", '');
        if ($res === false) {
            return false;
        }
        $res = json_decode($res, true);
        if (isset($res['error'])) {
            $this->set_error(json_encode($res['error']));
            return false;
        } else {
            return true;
        }
    }


    /*
     * count() 统计符合条件的文档数量
     *
     * @return false, 查询语句或者数据访问有错误, 调用 error() 查看； 成功返回数组, 如果没有被匹配到，返回 []
     */
    public function count($query = null)
    {
        $url = $this->build_url("_count");
        $post_fields = $this->build_query($query);
        $data = $this->curl($url, "GET", $post_fields);
        if ($data === false) {
            return false;
        }
        $res = json_decode($data, true);
        if (!$data) {
            $this->set_error("wrong data:" . $data);
            return false;
        }
        if (isset($res['error'])) {
            $this->set_error("error:" . json_encode($res['error']));
            return false;
        }
        return $res['count'];
    }


    /* 
     * search() 查询文档
     *
     * @return false, 查询语句或者数据访问有错误, 调用 error() 查看； 成功返回数组, 如果没有被匹配到，返回 []
     */
    public function search($query = null)
    {
        $url = $this->build_url("_search");
        $post_fields = $this->build_query($query);
        $data = $this->curl($url, "GET", $post_fields);
        if ($data === false) {
            return false;
        }
        $data = json_decode($data, true);
        return $this->format_output($data);
    }


    /*
     * bulk_upsert 批量插入数据
     *
     * @param   $data,  数据, [["field1" => "value" , "field2" => "value2"], ... ]
     * @param   $_id,   作为文档_id的键值，字符串 或者 数组； 数组表示由一个组合键值来作为 文档的'_id', 文档'_id' 组合成功以后的格式为 "." 分割的字符串，形如"{val1}.{val2}.{val3}...."
     */
    public function bulk_upsert(array $data, $_id)
    {
        if (is_string($_id)) {
            $_id = [$_id];
        }
        $url = "http://" . $this->host . ":" . $this->port . "/_bulk";
        //$index_res = $this->curl($url, "POST", $this->build_bulk_body($data, "index", $_id));
        $result = $this->curl($url, "POST", $this->build_bulk_body($data, "update", $_id));
        $result = json_decode($result, true);

        $update_succ = $create_succ = $failed = [];

        $update_succ = $this->fetch_result($result, "update", 200);

        if ($result['errors'] == 1) {
            $update_failed = $this->fetch_result($result, "update", 200, false);
            $update_failed = array_flip($update_failed);
            foreach ($data as $d) {
                $_id_key = $this->fetch_id_key($d, $_id);
                if (isset($update_failed[$_id_key])) {
                    $update_failed[$_id_key] = $d;
                }
            }

            $result = $this->curl($url, "POST", $this->build_bulk_body($update_failed, "create", $_id));
            $result = json_decode($result, true);

            $create_succ = $this->fetch_result($result, "create", 201);
            if ($result['errors'] == 1) {
                $failed = $this->fetch_result($result, "create", 201, false);
            }
        }

        return [
            "update" => $update_succ,
            "create" => $create_succ,
            "failed" => $failed,
        ];
    }

    /* 
     * nested() 对应 ES 的 nested query
     *
     * @param   $path, nested 的 path
     * @param   $func, 原型为 function(Es $es)
     * @return  $this
     */
    public function nested($path, \Closure $func)
    {
        $this->set_nested($path);
        $this->push_bool();
        $func($this);
        $nested = [
            "nested" => [
                "path" => $path,
                $this->context => $this->curr_bool,
            ],
        ];
        $this->pop_bool($nested);
        return $this;
    }

    /*
     * must() 对应 ES 的must查询语句, 相当于逻辑 AND
     *
     * @param   $func, 原型为 function(Es $es)
     * @return  $this;
     */
    public function must(\Closure $func)
    {
        /* push some pre-operations into stack*/
        $this->push_bool();
        $func($this);
        $must = [
            "bool" => [
                "must" => $this->curr_bool,
            ],
        ];
        $this->pop_bool($must);
        return $this;
    }

    /*
     * should() 对应 ES 的should查询语句, 相当于逻辑 OR
     *
     * @param   $func, 原型为 function(Es $es)
     * @return  $this;
     */
    public function should(\Closure $func)
    {
        $this->push_bool();
        $func($this);
        $should = [
            "bool" => [
                "should" => $this->curr_bool,
            ],
        ];
        $this->pop_bool($should);
        return $this;
    }

    /*
     * must_not() 对应 ES 的must_not查询语句, 相当于逻辑 AND NOT
     *
     * @param   $func, 原型为 function(Es $es)
     * @return  $this;
     */
    public function must_not(\Closure $func)
    {
        $this->push_bool();
        $func($this);
        $must_not = [
            "bool" => [
                "must_not" => $this->curr_bool,
            ],
        ];
        $this->pop_bool($must_not);
        return $this;
    }

    private function parse_nested_path($field)
    {
        foreach ($this->nested_path as $path => $null) {
            if (strpos($field, $path) === 0) {
                return [
                    "path" => $path,
                    "field" => substr($field, strlen($path) + 1)
                ];
            }
        }
        return false;
    }

    /*
     * where() 对应 ES 的term, range, 和 terms语句
     *      $op = "=",  对应 term, 注意当字段是个多值字段时，term的意思将变成“包含”而不是“等于”
     *      $op = "<", ">", "<=", ">=" 对应 range
     *      $op = "in", 对应 terms
     *
     * @param   $field, 字段名
     * @param   $op,    比较操作，有 "=", ">=", "<=", ">", "<", 和 "in"
     * @param   $value, 字段值
     */
    public function where($field, $op, $value)
    {
        if ($info = $this->parse_nested_path($field)) {
            list($nested_path, $nested_field) = array_values($info);
            $this->set_nested($nested_path, "where", ["op" => $op, "val" => $value, "field" => $nested_field]);

            /* _source[] 为空 或者 _source[] 中有该字段时，需要返回给调用者 */
            if (!isset($this->_source[$field])) {
                $this->_source[$field][] = $field;
            }
        }
        $this->switch_to_context("filter");
        $this->wrap_bool("must");
        $filter = [];
        switch ($op) {
        case "=":
            $filter["term"] = [$field => $value];
            break;

        case "in":
            if (!is_array($value)) {
                throw new \Exception(" 'in' operation should use an array as 'value'");
            }
            $filter["terms"] = [$field => $value];
            break;

        case ">=":
            if (!is_numeric($value)) {
                throw new \Exception(" '>=' operation should use number as 'value'");
            }
            $filter["range"] = [$field => ["gte" => $value]];
            break;

        case "<=":
            if (!is_numeric($value)) {
                throw new \Exception(" '<=' operation should use number as 'value'");
            }
            $filter["range"] = [$field => ["lte" => $value]];
            break;

        case "<":
            if (!is_numeric($value)) {
                throw new \Exception(" '<' operation should use number as 'value'");
            }
            $filter["range"] = [$field => ["lt" => $value]];
            break;

        case ">":
            if (!is_numeric($value)) {
                throw new \Exception(" '>' operation should use number as 'value'");
            }
            $filter["range"] = [$field => ["gt" => $value]];
            break;

        default :
            throw new \Exception(" unknown operation '$op'");

        }
        $this->curr_bool[] = $filter;
        return $this;
    }

    /*
     * exists() 对应 ES 的 exists 和 missing
     *
     * @param   $field, 字段名
     * @param   $exists,  true or false
     */
    public function exists($field, $exists = true)
    {
        $this->switch_to_context("filter");
        $this->wrap_bool("must");
        if ($exists) {
            $filter = ["exists" => ["field" => $field]];
        } else {
            $filter = ["missing" => ["field" => $field]];
        }
        $this->curr_bool[] = $filter;
        return $this;
    }

    /*
     * match() 对应 ES 的match语句
     *
     * @param   $field, 字段名
     * @param   $keywords,  查询关键字
     * @param   $boost, 权重, 默认为1
     */
    public function match($field, $keywords, $boost = 1)
    {
        $this->switch_to_context("query");
        $this->wrap_bool("should");
        if ($boost == 1) {
            $condition = $keywords;
        } else {
            $condition = ["query" => $keywords, "boost" => $boost];
        }
        $query = [
            "match" => [$field => $condition]
        ];
        $this->curr_bool[] = $query;
        return $this;
    }

    /*
     * sort() 根据字段排序
     * 要注意使用多个字段排序时，是会根据 sort() 调用的顺序来确定 字段的优先级的
     * 例如: ->sort("date")->sort("price") 会先按照 "date" 排序，当 date 相等时，再按照 "price" 排序
     *
     * @param   $field, 字段名
     * @param   $order, 排序方式，"desc" 为倒序, "asc" 为升序, 不区分大小写
     */
    public function sort($field, $order = "desc")
    {
        $this->sort[] = [$field => ["order" => $order]];
        return $this;
    }

    /*
     * sort_score() 启用分数排序
     * 相当于 sort("_score", "desc")
     */
    public function sort_score()
    {
        $this->sort("_score", "desc");
        return $this;
    }

    /*
     * to_query() 返回query数据，用于debug
     */
    public function to_query()
    {
        if (!empty($this->curr_bool)) {
            $this->unwrap_bool();
        }

        $query = [];
        if (!empty($this->query)) {
            if (!empty($this->filter)) {
                $query = [
                    "query" => [
                        "filtered" => [
                            "query" => $this->query,
                            "filter" => $this->filter,
                        ],
                    ]
                ];
            } else {
                $query = [
                    "query" => $this->query
                ];
            }
        } else {
            if (!empty($this->filter)) {
                $query = [
                    "filter" => $this->filter,
                ];
            } else {
                $query = [
                    "query" => ["match_all" => []]
                ];
            }
        }
        if (!empty($this->sort)) {
            $query['sort'] = $this->sort;
        }
        if (!empty($this->_source)) {
            $query['_source'] = array_keys($this->_source);
        }
        return $query;
    }

    private function clean_query()
    {
        $this->query = $this->filter = $this->sort = $this->cache = [];
        $this->context = null;
        $this->_source = $this->meta = [];
        $this->no_select = true;
        $this->from = $this->size = 0;
        $this->status = null;
        $this->nested_path = $this->bool_stack = $this->curr_bool = [];
    }

    private function curl($url, $method, $post_fields=null)
    {
        $ch = curl_init($url);
        if (!$ch) {
            $this->set_error("failed curl_init('$url')");
            return false;
        }
        $curl_options = [
            CURLOPT_RETURNTRANSFER => 1,
        ];
        $this->set_method($curl_options, $method);
        if ($post_fields) {
            $curl_options[CURLOPT_POSTFIELDS] = $post_fields;
        }
        $conn_timeout_ms = intval($this->conntimeout_ms);
        $timeout_ms = intval($this->timeout_ms);
        if ($conn_timeout_ms > 0) {
            $curl_options[CURLOPT_CONNECTTIMEOUT_MS] = $conn_timeout_ms;
        }
        if ($timeout_ms > 0) {
            $curl_options[CURLOPT_TIMEOUT_MS] = $timeout_ms;
        }
        if (curl_setopt_array($ch, $curl_options) === false) {
            $this->set_error("failed curl_setopt_array(" . json_encode($curl_options) . ")");
            curl_close($ch);
            return false;
        }
        $res = curl_exec($ch);
        if ($res === false) {
            $this->error = "failed curl_exec($url). error:" . curl_error($ch);
            curl_close($ch);
            return false;
        }
        curl_close($ch);

        return $res;
    }

    private function set_method(&$curl_options, $method)
    {
        $method = strtoupper($method);
        switch ($method) {
        case "POST":
            $curl_options[CURLOPT_POST] = 1;
            break;

        default:
            $curl_options[CURLOPT_CUSTOMREQUEST] = $method;
            break;
        }
    }

    private function set_error($errmsg)
    {
        $this->error = $errmsg;
        return false;
    }

    /* unpack_nested() 将nested结构的数据打散成扁平状
     */
    private function unpack_nested_data($data)
    {
        $result = [];
        $tmp = $data;
        foreach ($data as $key => $element) {
            if ($this->is_nested_path($key) && is_array($element)) {
                $where = $this->get_nested($key, "where");
                foreach ($element as $e) {
                    if (!$where || $this->nested_compare($where, $e)) {
                        $tmp[$key] = $e;
                        $result[] = $tmp;
                    }
                }
            }
        }
        if (!$result) {
            return [$data];
        } else {
            return $result;
        }
    
    }

    private function nested_compare($where, $element)
    { 
        foreach ($where as $w) {
            $res = $this->nested_compare_op($w, $element);
            if (!$res) {
                return false;
            }
        }
        return true;
    }

    private function nested_compare_op($where_op, $element)
    {
        $field = $where_op['field'];
        if (!isset($element[$field])) {
            return false;
        }
        $val = $element[$field];

        switch ($where_op['op']) {
        case '=':
            return $val == $where_op['val'];
        case 'in':
            return in_array($val, $where_op['val']);
        case '>':
            return $val > $where_op['val'];
        case '<':
            return $val < $where_op['val'];
        case '>=':
            return $val >= $where_op['val'];
        case '<=':
            return $val <= $where_op['val'];
        }
        return false;
    }

    private function get_nested($path, $tag)
    {
        if (isset($this->nested_path[$path][$tag])) {
            return $this->nested_path[$path][$tag];
        }
    }

    private function set_nested($path, $tag = null, $op = null)
    {
        if (!isset($this->nested_path[$path])) {
            $this->nested_path[$path] = [];
        }
        switch ($tag) {
        case "where":
            $this->nested_path[$path]['where'][] = $op;
            break;
        default:
            break;
        }
    }

    private function is_nested_path($path)
    {
        return isset($this->nested_path[$path]);
    }

    private function format_output($data)
    {
        $return = [];
        if (isset($data['hits']['hits'])) {
            if (!empty($this->nested_path)) {
                $hits = [];
                foreach ($data['hits']['hits'] as $d) {
                    $d_sources = $this->unpack_nested_data($d['_source']);
                    foreach ($d_sources as $source) {
                        $d['_source'] = $source;
                        $hits[] = $d;
                    }
                }
                $data['hits']['hits'] = $hits;
            }
            foreach ($data['hits']['hits'] as $d) {
                $row = $d['_source'];

                if ($this->no_select) {
                    $return[] = $row;
                    continue;
                }

                $new_row = [];
                if (!empty($this->meta)) {
                    foreach ($this->meta as $key => $alias) {
                        if (isset($d[$key])) {
                            $val = $d[$key];
                        } else {
                            $val = false;
                        }
                        foreach ($alias as $a) {
                            $new_row[$a] = $val;
                        }
                    }
                }

                if (!empty($this->_source)) {
                    foreach ($this->_source as $key => $alias) {
                        $val = $this->fetch_deep_element($d['_source'], explode(".", $key));
                        foreach ($alias as $rename) {
                            if (isset($this->fields_to_user[$key])) {
                                $new_row[$rename] = $val;
                            }
                        }
                    }
                }
                $return[] = $new_row;
            }
        }
        return $return;
    }

    private function fetch_deep_element(array $source, array $key_chain)
    {
        $key = array_shift($key_chain);
        if (!isset($source[$key])) {
            return null;
        } elseif (empty($key_chain)) {
            return $source[$key];
        } elseif (!is_array($source[$key])) {
            return null;
        } else {
            return $this->fetch_deep_element($source[$key], $key_chain);
        }
    }

    private function build_query($query)
    {
        if ($query !== null) {
            $post_fields = json_encode($query);
        } else {
            $post_fields = json_encode($this->to_query());
        }
        return $post_fields;
    }

    private function build_url($operation)
    {
        $path = '';
        if ($this->index) {
            $path .= $this->index . "/";
        }
        if ($this->type) {
            $path .= $this->type . "/";
        }

        $get = [];
        if ($this->size) {
            $get['size'] = $this->size;
        }
        if ($this->from) {
            $get['from'] = $this->from;
        }

        $head = "http://" . $this->host . ":" . $this->port . "/" ;
        switch ($operation) {
        case "":
            return $head . $this->index;

        case "_bulk":
            return $head . $operation;

        case "_mapping":
        case "_alias":
            return $head . $this->index . "/" . $operation;

        case "_count":
        case "_search":
            return $head . $path . $operation . "?" . http_build_query($get);
        }
    }

    private function fetch_result(array $result, $operation, $code, $equal = true)
    {
        $return = [];
        if (isset($result['error'])) {
            $this->set_error(json_encode($result));
            return $return;
        }
        foreach ($result['items'] as $r) {
            if ($equal) {
                $check = ($r[$operation]['status'] == $code);
            } else {
                $check = ($r[$operation]['status'] != $code);
            }
            if ($check) {
                $return[] = $r[$operation]['_id'];
            }
        }
        return $return;
    }

    private function build_bulk_body(array $data, $operation, $_id)
    {
        $body = [];
        foreach ($data as $d) {
            $row = $d;
            $d = [];
            $d['_index'] = $this->index;
            $d['_type'] = $this->type;
            $d['_id'] = $this->fetch_id_key($row, $_id);
            $body[] = json_encode([$operation => $d]);
            if ($operation == "update") {
                $body[] = json_encode(["doc" => $row]);
            } else {
                $body[] = json_encode($row);
            }
        }
        return implode("\n", $body) . "\n";
    }

    private function fetch_id_key($row, array $_id_union)
    {
        $_id = [];
        foreach ($_id_union as $key) {
            if (!isset($row[$key])) {
                throw new \Exception("unknown _id of document: '$key'");
            }
            $_id[] = $row[$key];
        }
        return implode(".", $_id);
    }

    private function put_deep_properties(&$array, array $keys, $val)
    {
        $k = array_shift($keys);
        if (!isset($array['properties'][$k])) {
            $array['properties'][$k] = [];
        }
        if (empty($keys)) {
            $array['properties'][$k] = $val;
        } else {
            $this->put_deep_properties($array['properties'][$k], $keys, $val);
        }
    }

    private function get_type_mapping($data_type)
    {
        switch ($data_type) {
        case 'string':
            return ["type" => "string", "index" => "not_analyzed"];

        case 'long':
            return ['type' => 'long'];

        case 'integer':
            return ['type' => 'integer'];

        case 'bool':
            return ['type' => 'bool'];

        case 'nested':
            return ['type' => 'nested'];

        default:
            return $data_type;
        }
    }

    private function pop_bool($bool_query)
    {
        $pre_bools = array_pop($this->bool_stack);
        if ($pre_bools) {
            $query = array_merge($pre_bools['op'], [$bool_query]);
        } else {
            $query = [$bool_query];
        }
        
        if (empty($this->bool_stack)) {
            switch ($this->context) {
            case "query":
                $this->query = $this->merge_bool($query[0], $this->query);
                break;

            case "filter":
                $this->filter = $this->merge_bool($query[0], $this->filter);
                break;

            default:
                throw new \Exception("wrong context : '{$this->context}'");
            }
            $this->curr_bool = [];
        } else {
            $this->curr_bool = $query;
        }
    }

    private function wrap_bool($bool, $force = false)
    {
        /* 当最外层嵌套没有时，需要自动加上(例如直接调用 (new ES)->where(..)->where(..) */
        if (empty($this->bool_stack)) {
            $this->push_bool($bool);
        }
    }

    private function push_bool($bool = null)
    {
        $this->bool_stack[] = ['bool' => $bool, 'context' => $this->context, 'op' => $this->curr_bool];
        $this->curr_bool = [];
    }

    private function merge_bool($merge_from, $merge_into)
    {
        if (isset($merge_from['bool'])) {
            foreach ($merge_from['bool'] as $bool => $info) {
                if (!isset($merge_into['bool'][$bool])) {
                    $merge_into['bool'][$bool] = [];
                }
                $merge_into['bool'][$bool] = array_merge($merge_into['bool'][$bool], $info);
            }
        } elseif (isset($merge_from['nested'])) {
            $merge_into = array_merge($merge_into, $merge_from);
        }
        return $merge_into;
    }

    private function switch_to_context($context)
    {
        switch ($this->context) {
        case null:
            $this->context = $context;
            break;

        case "query":
        case "filter":
            $this->context = $context;
            break;
        }
    }

    private function unwrap_bool()
    {
        if (empty($this->bool_stack)) {
            return;
        }
        $bool_info = array_pop($this->bool_stack);
        $bool = $bool_info['bool'];
        array_push($this->bool_stack, $bool_info);
        $query = [
            "bool" => [
                $bool => $this->curr_bool
            ]
        ];
        $this->pop_bool($query);
    }
}
