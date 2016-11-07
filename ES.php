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

    private $query = [];
    private $sort = [];
    private $filter = [];
    private $cache = [];

    //标记当前的 must/should/must_not 所处的上下文
    //null 为初始化, "*" 为未知, "filter", "query" 分别对应过滤和查询
    private $context = null;   
    private $context_level = 0;


    public function __construct($conf = null)
    {
        if ($conf) {
            $this->host = $conf['host'];
            $this->port = $conf['port'];
            $this->index = $conf['index'];
        }
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
        foreach ($fields as $f) {
            if (is_string($f)) {
                if (preg_match('/^([\w\.]+)\s+as\s+(\w+)$/i', $f, $match)) {
                    $this->_source[$match[1]][] = $match[2];
                } else {
                    $this->_source[$f][] = $f;
                }
            }
        }
        return $this;
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
     * must() 对应 ES 的must查询语句, 相当于逻辑 AND
     *
     * @param   $func, 原型为 function(Es $es)
     * @return  null;
     */
    public function must(\Closure $func)
    {
        $this->before("must");
        $func($this);
        $this->after();
        return $this;
    }

    /*
     * should() 对应 ES 的should查询语句, 相当于逻辑 OR
     *
     * @param   $func, 原型为 function(Es $es)
     * @return  null;
     */
    public function should(\Closure $func)
    {
        $this->before("should");
        $func($this);
        $this->after();
        return $this;
    }

    /*
     * must_not() 对应 ES 的must_not查询语句, 相当于逻辑 AND NOT
     *
     * @param   $func, 原型为 function(Es $es)
     * @return  null;
     */
    public function must_not(\Closure $func)
    {
        $this->before("must_not");
        $func($this);
        $this->after();
        return $this;
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
        $this->try_to_switch_context("filter", "must");
        $this->try_to_push_context("filter", "where");
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
        $this->cache[] = $filter;
        $this->pop_context();
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
        $this->try_to_switch_context("filter", "must");
        $this->try_to_push_context("filter", "exists");
        if ($exists) {
            $filter = ["exists" => ["field" => $field]];
        } else {
            $filter = ["missing" => ["field" => $field]];
        }
        $this->cache[] = $filter;
        $this->pop_context();
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
        $this->try_to_switch_context("query", "should");
        $this->try_to_push_context("query", "match");
        if ($boost == 1) {
            $condition = $keywords;
        } else {
            $condition = ["query" => $keywords, "boost" => $boost];
        }
        $query = [
            "match" => [$field => $condition]
        ];
        $this->cache[] = $query;
        $this->pop_context();
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
        $this->after();
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

    private function try_to_push_context($tag, $func_name)
    {
        if ($this->context != "*" && $this->context != null) {
            if ($tag != "*" && $tag != $this->context) {
                throw new \Exception("could not call $func_name() in a $this->context context");
            }
        }
        if ($this->context == "*" || $this->context == null) {
            $this->context = $tag;
        }
        $this->context_level += 1;
    }

    private function build_query_from_cache(array &$cache)
    {
        $query = [];
        while (!empty($cache)) {
            $op = array_shift($cache);
            if (in_array($op, ["must", "must_not", "should"])) {
                $query[] = ["bool" => [$op => $this->build_query_from_cache($cache)]];
            } elseif ($op == "end") {
                return $query;
            } else {
                $query[] = $op;
            } 
        }
        if (empty($query)) {
            return [];
        }
        return $query[0];
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
        }
        return $merge_into;
    }

    private function pop_context()
    {
        $this->context_level -= 1;
        if ($this->context_level == 0) {
            switch ($this->context) {
            case "query":
                $this->query = $this->merge_bool(
                    $this->build_query_from_cache($this->cache), 
                    $this->query
                );
                break;

            case "filter":
                $this->filter = $this->merge_bool(
                    $this->build_query_from_cache($this->cache, $this->filter),
                    $this->filter
                );
                break;
            }

            $this->context = null;
        }
    }

    private function clean_query()
    {
        $this->query = $this->filter = $this->sort = [];
        $this->cache = [];
        $this->context = null;
        $this->context_level = 0;
    }

    private function before($bool)
    {
        $this->try_to_push_context("*", $bool);
        $this->cache[] = $bool;
    }

    private function after()
    {
        if ($this->context_level) {
            $this->cache[] = "end";
            $this->pop_context();
        }
    }

    private function try_to_switch_context($to_context, $bool)
    {
        $curr_ctx = $this->context;
        if ($curr_ctx != "*" && $curr_ctx != $to_context && $curr_ctx != null) {
            $this->after();
            $this->before($bool);
        }
        if ($curr_ctx == null) {
            $this->before($bool);
        }

    }

    private function curl($url, $method, $post_fields)
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
        $curl_options[CURLOPT_POSTFIELDS] = $post_fields;
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
            curl_close($ch);
            $this->error = "failed curl_exec($url). error:" . curl_error($ch);
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

    private function format_output($data)
    {
        $return = [];
        if (isset($data['hits']['hits'])) {
            foreach ($data['hits']['hits'] as $d) {
                $row = $d['_source'];
                $new_row = [];
                foreach ($this->_source as $key => $alias) {
                    foreach ($alias as $rename) {
                        if (false === strpos($key, ".")) {
                            $new_row[$rename] = $d['_source'][$key];
                        } else {
                            $new_row[$rename] = $this->fetch_deep_element($d['_source'], explode(".", $key));
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
            return false;
        } elseif (empty($key_chain)) {
            return $source[$key];
        } elseif (!is_array($source[$key])) {
            return false;
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
        case "_bulk":
            return $head . $operation;

        case "_count":
        case "_search":
            return $head . $path . $operation . "?" . http_build_query($get);
        }
    }

    private function fetch_result(array $result, $operation, $code, $equal = true)
    {
        $return = [];
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
}
