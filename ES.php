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
    const MAX_SEARCH_NUM = 9999;
    const INDEX_EXISTS = 1;
    const INDEX_NOT_EXISTS = 0;
    const INDEX_UNKNOWN = -1;

    private static $meta_fields = [
        "_score", "_index", "_type", "_id", "_source"
    ];

    public $host = '127.0.0.1';
    public $port = 9200;
    public $user = null;
    public $pass = null;

    public $timeout_ms = 0;    //curl函数超时时间，单位是毫秒, <= 0 表示不超时
    public $conntimeout_ms = 0;    //连接超时时间，单位是毫秒, <= 0 表示不超时
    public $retry_num = 3;  //重试次数

    private $error = null;

    private $index = null;
    private $type = null;

    private $size = self::MAX_SEARCH_NUM;  //分页大小, 默认不设置
    private $from = 0;   //默认从第一条开始

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

    /*
     * nested 信息，保存每个nested节点的逻辑过滤条件，用于 filter_nested()  结构示例：
     * [
            "e.f" => [
                "must" => [
                    ["d", ">=", 2],
                    "should" => [
                        ["d", "!=", 0],
                        ["d", "<", 4],
                    ],
                    ["d", "<", 3],
                ],
            ],
            "b" => [
                "should" => [
                    ["c", "!=", "3"],
                ],
            ]
     * ]
     */
    private $nested = [];

    /*
     * nested_bool 字段, 该字段用于保存 nested 里面嵌套的 bool(must, should, must_not) 状态:
     * 
     * $es->nested("e.f", function($es) {
     *      $es->must(function($es) {
     *          ...
     *          $es->should(function($es) {
     *              ...
     *          });
     *      });
     * });
     * 对应结构示例:
     *
     * [
     *      "e.f" => ["must", "should"],
     * ]
     *
     */
    private $nested_bool = [];

    /*
     * current_nested 字段，该字段用于 nested 嵌套调用时，保存 nested 状态，
     * $es->nested("b", function($es) {
     *      ...
     *      $es->nested("b.e.f", function($es) {
     *          ...
     *      }
     * });
     * 对应结构示例:
     *
     * ["b.e.f", "b"]
     *
     */
    private $curr_nested = [];

    private $select = [];


    private $scroll_id = null;
    private $scroll_maxtime = '1m';
    private $cluster = [];

    private $min_score = null;
    private $disable_coord = false;

    public function __construct($conf = null)
    {
        if ($conf) {
            $this->host = $conf['host'];
            $this->port = $conf['port'];
            $this->index = $conf['index'];
        }
    }

    /*
     * disable_coord()
     */
    public function disable_coord()
    {
        $this->disable_coord = true;
        return $this;
    }

    /*
     * min_score()
     */
    public function min_score($min_score)
    {
        $this->min_score = $min_score;
        return $this;
    }

    /*
     * set_cluster() 设置访问集群
     * @param   $cluster,  数组，每个元素为一个节点： ["host"=>x, "port"=>x, "user"=>x, "pass"=>x]
     * 每个节点按顺序访问，第一个不成功换第二个，第二个不成功换第三个...., 以此类推
     */
    public function set_cluster(array $cluster)
    {
        if (empty($cluster)) {
            throw new \Exception("Es::set_cluster(), empty cluster, check param");
        }
        foreach ($cluster as $i => $c) {
            $c['host'] = isset($c['host']) ? $c['host'] : '127.0.0.1';
            $c['port'] = isset($c['port']) ? $c['port'] : '9200';
            $c['user'] = isset($c['user']) ? $c['user'] : null;
            $c['pass'] = isset($c['pass']) ? $c['pass'] : null;
            $cluster[$i] = $c;
        }
        $this->cluster = $cluster;
        $this->host = $cluster[0]['host'];
        $this->port = $cluster[0]['port'];
        $this->pass = $cluster[0]['pass'];
        $this->user = $cluster[0]['user'];
        return $this;
    }

    /*
     * get_type_name() 获取type名
     */
    public function get_type_name()
    {
        return $this->type;
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
        foreach ($fields as $f) {
            if (is_string($f)) {
                if (!preg_match('/^([\w_.-]+)\s+as\s+([^\s]+)$/', $f, $farr)) {
                    $field = $f;
                    $alias = $f;
                } else {
                    $field = $farr[1];
                    $alias = $farr[2];
                }
                $this->select[$field] = $alias;
            }
        }
        return $this;
    }

    /*
     * get_by_id()   根据 _id 获取document, 该方法比 search() 要高效：https://discuss.elastic.co/t/difference-between-get-by-document-id-and-running-a-query-matching-on-document-id/62313
     *
     * @return  false, 请求发生错误，调用 error() 查看;
     *          null,  该document不存在
     *          array, 该document存在
     */
    public function get_by_id($documentId)
    {
        $res = $this->curl($this->build_url("get_by_id") . $documentId, "GET");
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        if ($data['found'] == false) {
            return null;
        }
        return $data['_source'];
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
     * delete()  删除索引
     */
    public function delete()
    {
        $res = $this->curl($this->build_url(""), "DELETE");
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
    }

    /*
     * create()  创建索引
     */
    public function create(array $settings = []) {
        $url = $this->build_url("");
        $post = [];
        if ($settings) {
            $post['settings'] = $settings;
        }
        if (!empty($post)) {
            $res = $this->curl($url, "PUT", json_encode($post));
        } else {
            $res = $this->curl($url, "PUT");
        }
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
    }

    /*
     * drop() 删除整个索引
     */
    public function drop_index()
    {
        $url = $this->build_url("");
        $res = $this->curl($url, "DELETE");
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
    }

    /*
     * settings() 获取/设置 settings
     *
     * @param   $settings, 需要设置的settings
     * @return  $settings
     */
    public function settings(array $settings = [])
    {
        $url = $this->build_url("_settings");
        if ($settings) {
            $this->curl($url . "/", "PUT", json_encode($settings));
        }
        $res = $this->curl($url, "GET");
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
    }

    /*
     * health() 获取集群的健康情况
     * 
     * @return  false, 获取失败；array 
     */
    public function health($only_this_index = true)
    {
        $url = $this->build_url("_cluster") . "/health";
        if ($only_this_index) {
            $url .= "/" . $this->index . "?level=indices";
        }
        $res = $this->curl($url, "GET");
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
    }

    /*
     * info() 获取当前索引的信息
     *
     * @return  false, 获取失败； array 当前索引的信息
     */
    public function info()
    {
        $url = $this->build_url("");
        $res = $this->curl($url, "GET");
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
    }

    /*
     * set_row_mappings() 设置原始格式的mapping
     *
     * @return false,  设置失败  
     */
    public function set_row_mapping(array $mappings)
    {
        $url = $this->build_url("_mapping");
        $this->curl($url . "/" . $this->type, "PUT", json_encode($mappings));
        $res = $this->curl($url, "GET");
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
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
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $data;
    }

    /*
     * re_alias() 重定向索引
     *
     * @param  $from_index,  原索引
     * @param  $to_index,  重新定向后的索引
     */
    public function re_alias($from_index, $to_index)
    {
        $url = $this->build_url("_aliases");
        $post_fields = [
            "actions" => [
                ["remove" => ["index" => $from_index, "alias" => $this->index]],
                ["add" => ["index" => $to_index, "alias" => $this->index]],
            ],
        ];
        $res = $this->curl($url, "POST", json_encode($post_fields));
        return $this->is_ok($res);
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
        return $this->is_ok($res);
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
        $data = $this->curl($url, "GET", $post_fields, true);
        if (!$this->is_ok($data, $res)) {
            return false;
        }
        return $res['count'];
    }


    /*
     * scroll_maxtime() 滚屏时，每批文档搜索的超时时间
     *
     * @param   $max_time,   1m, 1h, 1s, 1d  见文档:https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units
     */
    public function scroll_maxtime($max_time)
    {
        $this->scroll_maxtime = $max_time;
        return $this;
    }

    /*
     * scroll() 滚屏接口
     *
     * @param   $size,   每一批返回的文档数， 这里可以超过 _search 的最大值 10000
     */
    public function scroll($size)
    {

        if (!$this->scroll_id) {
            $url = $this->build_url("scroll_search");
            $post_fields = $this->build_query(null, false);
            $post_fields['size'] = intval($size);

        } else {
            $url = $this->build_url("scroll");
            $post_fields = [
                'scroll_id' => $this->scroll_id,
                'scroll' => $this->scroll_maxtime,
            ];
        }
        $post_fields = json_encode($post_fields);

        $res = $this->curl($url, "GET", $post_fields);
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        $this->scroll_id = $data['_scroll_id'];
        return $this->format_output($data);
    }

    /* 
     * search() 查询文档
     *
     * @return false, 查询语句或者数据访问有错误, 调用 error() 查看； 成功返回数组, 如果没有被匹配到，返回 []
     */
    public function search()
    {
        $url = $this->build_url("_search");
        $post_fields = $this->build_query(null);
        $res = $this->curl($url, "GET", $post_fields, true);
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $this->format_output($data);
    }

    /*
     * aggs() 聚合
     *
     * @param   $buckets 需要聚合的fields值，该参数是嵌套的形式, 'bucket1' => 'bucket2' => 'metric' 的形式出现
     * 示例:
     *      ["channel" => "terms"]             按 channel 聚合
     *      ["cnannel" => ["name" => "terms"]] 先按 channel 聚合， 然后在此基础上再按 name 聚合
     *      ["channel" => "avg"]   按 channel 聚合计算平均值
     *      ["cnannel" => ["name" => "avg"]] 先按 channel 聚合， 然后在此基础上再按 name 聚合, 并计算平均值
     *
     *      目前ES支持的 metric 有 avg,  sum,  max, min, terms
     *
     * @return false, 查询语句或者数据访问失败, 调用 error() 查看;   成功返回数据
     * 返回示例:
     *
     *      [
     *          "channel" => [
     *              "c1" => [
     *                  "name" => [
     *                      "name1" => 11,
     *                      "name2" => 12,
     *                      "name3" => 13
     *                  ]
     *              ],
     *              "c2" => [
     *                  ...
     *              ]
     *          ]
     *     ]
     */
    public function aggs(array $buckets, $aggs_size = 10)
    {
        /* 这里我们不需要拿每个document的具体数据，所以设置为最小值 1 */
        $this->size = 1;

        $url = $this->build_url("_search");
        $post_fields = $this->build_query(null, false);
        $post_fields['aggs'] = $this->aggs_query($buckets, $aggs_size);

        $res = $this->curl($url, "GET", json_encode($post_fields), true);
        if (!$this->is_ok($res, $data)) {
            return false;
        }
        return $this->aggs_group($data['aggregations'], $buckets);
    }

    private function aggs_group(array $aggs, array $fields)
    {
        $group = [];
        foreach ($fields as $field => $nested_field) {
            $nested = $aggs[$field];
            if (is_array($nested_field)) {
                $buckets = $nested['buckets'];
                foreach ($buckets as $buck) {
                    $key = $buck['key'];
                    unset($buck['key']);
                    unset($buck['doc_count']);
                    $group[$field][$key] = $this->aggs_group($buck, $nested_field);
                }
            } elseif ($nested_field == "terms") {
                $buckets = $nested['buckets'];
                foreach ($buckets as $buck) {
                    $group[$field][$buck['key']] = $buck['doc_count'];
                }
            } else {
                $group[$field] = $aggs[$field]['value'];
            }
        }
        return $group;
    }


    private function aggs_query(array $buckets, $aggs_size)
    {
        $query = [];
        foreach ($buckets as $field => $q) {
            if (is_string($q)) {
                $query[$field][$q] = [
                    "field" => $field
                ];
                if ($q == "terms") {
                    $query[$field][$q]["size"] = $aggs_size;
                }
            } elseif (is_array($q)) {
                $query[$field]['terms'] = [
                    "size" => $aggs_size,
                    "field" => $field,
                ];
                $query[$field]['aggs'] = $this->aggs_query($q, $aggs_size);
            } else {
                throw new \Exception("wrong buckets:" . json_encode($q));
            }
        }
        return $query;
    }

    /*
     * bulk_update() 批量更新文档
     *
     * @return false, 更新失败； ["update" => [ids], "failed" => [ids]]  更新成功（部分可能会失败）
     */
    public function bulk_update(array $data, $_id)
    {
        if (is_string($_id)) {
            $_id = [$_id];
        }
        $url = $this->build_url("_bulk");
        $result = $this->curl($url, "POST", $this->build_bulk_body($data, "update", $_id));
        if ($result === false) {
            return false;
        }

        $result = json_decode($result, true);
        $update_succ = $failed = [];
        $update_succ = $this->fetch_result($result, "update", 200);
        $failed = $this->fetch_result($result, "update", 200, false);
        return [
            "update" => $update_succ,
            "failed" => $failed,
        ];
    }

    /*
     * bulk_delete() 批量删除文档
     *
     * @return false, 更新失败; ["delete" => [ids], "failed" => [ids]]  删除成功（部分可能失败）
     */
    public function bulk_delete(array $ids)
    {
        $url = $this->build_url("_bulk");
        $data = [];
        foreach ($ids as $id) {
            $data[] = ["_id" => $id];
        }
        $result = $this->curl($url, "POST", $this->build_bulk_body($data, "delete", "_id"));
        if ($result === false) {
            return false;
        }

        $result = json_decode($result, true);
        $update_succ = $failed = [];
        $update_succ = $this->fetch_result($result, "delete", 200);
        $failed = $this->fetch_result($result, "delete", 200, false);
        return [
            "delete" => $update_succ,
            "failed" => $failed,
        ];
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
        $url = $this->build_url("_bulk");
        $result = $this->curl($url, "POST", $this->build_bulk_body($data, "update", $_id));
        if ($result === false) {
            $result = [];
        } else {
            $result = json_decode($result, true);
        }

        $update_succ = $create_succ = $failed = [];
        $update_succ = $this->fetch_result($result, "update", 200);
        if (isset($result['errors']) && $result['errors'] == 1) {
            $update_failed = $this->fetch_result($result, "update", 200, false);
            $update_failed = array_flip($update_failed);
            foreach ($data as $d) {
                $_id_key = $this->fetch_id_key($d, $_id);
                if (isset($update_failed[$_id_key])) {
                    $update_failed[$_id_key] = $d;
                }
            }

            $result = $this->curl($url, "POST", $this->build_bulk_body($update_failed, "create", $_id));
            if ($result === false) {
                $result = [];
            } else {
                $result = json_decode($result, true);
            }
            $create_succ = $this->fetch_result($result, "create", 201);
            if (isset($result['errors'])) {
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
        $this->into_nested($path);
        $this->push_bool();
        $func($this);
        $nested = [
            "nested" => [
                "path" => $path,
                "query" => $this->curr_bool,
            ],
        ];
        $this->pop_bool($nested);
        $this->outof_nested();
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
        /* 1. 如果是在 nested 回调中，需要做 nested 的处理 */
        /* push some pre-operations into stack*/
        $this->tryto_push_nested_bool("must");
        $this->push_bool();
        $func($this);
        $must = [
            "bool" => [
                "must" => $this->curr_bool,
            ],
        ];
        $this->pop_bool($must);
        $this->tryto_pop_nested_bool();
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
        $this->tryto_push_nested_bool("should");
        $this->push_bool();
        $func($this);
        $should = [
            "bool" => [
                "should" => $this->curr_bool,
            ],
        ];
        $this->pop_bool($should);
        $this->tryto_pop_nested_bool();
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
        $this->tryto_push_nested_bool("must_not");
        $this->push_bool();
        $func($this);
        $must_not = [
            "bool" => [
                "must_not" => $this->curr_bool,
            ],
        ];
        $this->pop_bool($must_not);
        $this->tryto_pop_nested_bool();
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
        $this->tryto_put_nested($field, $op, $value);
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
            $this->must_be_able_to_compare('>=', $value);
            $filter["range"] = [$field => ["gte" => $value]];
            break;

        case "<=":
            $this->must_be_able_to_compare('<=', $value);
            $filter["range"] = [$field => ["lte" => $value]];
            break;

        case "<":
            $this->must_be_able_to_compare('<', $value);
            $filter["range"] = [$field => ["lt" => $value]];
            break;

        case ">":
            $this->must_be_able_to_compare('>', $value);
            $filter["range"] = [$field => ["gt" => $value]];
            break;

        default :
            throw new \Exception(" unknown operation '$op'");

        }
        $this->curr_bool[] = $filter;
        return $this;
    }

    private function must_be_able_to_compare($op, $value)
    {
        if (is_numeric($value) || strtotime($value)) {
            return true;
        }
        throw new \Exception(" '$op' operation should use number or a timestamp as 'value'");
    }

    /*
     * exists() 对应 ES 的 exists 和 missing
     *
     * @param   $field, 字段名
     * @param   $exists,  true or false
     */
    public function exists($field, $exists = true)
    {
        if ($exists) {
            return $this->find($field);
        } else {
            return $this->missing($field);
        }
    }

    /*
     * missing() 对应 ES 的 missing
     */
    public function missing($field)
    {
        return $this->must_not(function($es) use ($field) {
            $es->find($field);
        });
    }

    /*
     * find() 对应 ES 的 exists
     */
    public function find($field)
    {
        $this->tryto_put_nested($field, 'exists', 'NULL');
        $this->switch_to_context("filter");
        $this->wrap_bool("must");
        $filter = ["exists" => ["field" => $field]];
        $this->curr_bool[] = $filter;
        return $this;
    }


    /*
     * match_phrase()
     */
    public function match_phrase($field, $keywords, $boost = null)
    {
        $this->switch_to_context("filter");
        $this->wrap_bool("must");
        if ($boost === null) {
            $condition = $keywords;
        } else {
            $condition = ["query" => $keywords, "boost" => $boost];
        }
        $query = [
            "match_phrase" => [$field => $condition]
        ];
        $this->curr_bool[] = $query;
        return $this;
    }

    /*
     * match() 对应 ES 的match语句
     *
     * @param   $field, 字段名
     * @param   $keywords,  查询关键字
     * @param   $boost, 权重, 默认为1
     */
    public function match($field, $keywords, $boost = null)
    {
        $this->switch_to_context("query");
        $this->wrap_bool("must");
        if ($boost === null) {
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
        $this->sort[] = [$field => ["order" => strtolower($order)]];
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

        if ($this->disable_coord) {
            $this->query['bool']['disable_coord'] = true;
        }

        if (empty($this->query) && empty($this->filter)) {
            $query = [
                "query" => ["match_all" => []]
            ];
        } elseif (empty($this->filter)) {
            $query = [
                "query" => $this->query
            ];
        } elseif (empty($this->query)) {
            $query = [
                "query" => [
                    "bool" => [
                        "filter" => $this->filter,
                    ],
                ]
            ];
        } else {
            $query = [
                "query" => [
                    "bool" => [
                        "filter" => $this->filter,
                    ],
                ]
            ];
            if (isset($this->query['bool'])) {
                foreach ($this->query['bool'] as $bool => $op) {
                    $query['query']['bool'][$bool] = $op;
                }
            }
        }

        if (!empty($this->sort)) {
            $query['sort'] = $this->sort;
        }
        if (!empty($this->select)) {
            if (!$this->select_nested()) {
                $query['_source'] = array_keys($this->select);
            } else {
                $query['_source'] = array_merge(array_keys($this->select), array_keys($this->nested));
            }
        }
        if ($this->min_score !== null) {
            $query['min_score'] = $this->min_score;
        }
        return $query;
    }

    private function select_nested()
    {
        /* 查看是否有将nested的字段select进来*/
        foreach ($this->nested as $nested_field => $null) {
            foreach ($this->select as $select_field => $null) {
                if (strpos($select_field, $nested_field) === 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private function clean_query()
    {
        $this->query = $this->filter = $this->sort = $this->cache = [];
        $this->context = null;
        $this->from = 0;
        $this->size = self::MAX_SEARCH_NUM;
        $this->status = null;
        $this->bool_stack = $this->curr_bool = [];
        $this->nested = $this->nested_bool = $this->select = $this->curr_nested = [];
    }

    private function curl($url, $method, $post_fields = null, $allow_cluster = false)
    {
        if ($this->cluster && $allow_cluster) {
            foreach ($this->cluster as $node) {
                $this->host = $node['host'];
                $this->port = $node['port'];
                $this->user = $node['user'];
                $this->pass = $node['pass'];
                $res = $this->exec_curl($url, $method, $post_fields);
                if ($res !== false) {
                    return $res;
                }
            }
            return $res;
        } else {
            return $this->exec_curl($url, $method, $post_fields);
        }
    }

    private function exec_curl($url, $method, $post_fields=null)
    {
        $head = "http://" . $this->host . ":" . $this->port . "/";
        $ch = curl_init($head . $url);
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

        if ($this->user) {
            $curl_options[CURLOPT_USERPWD] = $this->user . ":" . $this->pass;
        }

        if ($timeout_ms > 0) {
            $curl_options[CURLOPT_TIMEOUT_MS] = $timeout_ms;
        }
        if (curl_setopt_array($ch, $curl_options) === false) {
            $this->set_error("failed curl_setopt_array(" . json_encode($curl_options) . ")");
            curl_close($ch);
            return false;
        }
        $res = false;
        $tryNum = 0;
        $errorNo = false;
        /* 如果是操作超时, 重试3次 (libcurl 没有 connect_timeout 的错误码，所以这里选择了一个最接近的 CURLE_OPERATION_TIMEDOUT */
        for ($i = intval($this->retry_num); $i > 0 && $res === false; $i --) {
            $tryNum += 1;
            $res = curl_exec($ch);
            $errorNo = curl_errno($ch);
            if ($errorNo != CURLE_OPERATION_TIMEDOUT && $errorNo != CURLE_COULDNT_CONNECT) {
                break;
            }
        }
        if ($res === false) {
            $this->error = "failed curl_exec($url). tryNum={$tryNum},errorNo={$errorNo},error:" . curl_error($ch);
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

    private function format_output($data)
    {
        $return = [];
        $total = 0;
        if (isset($data['hits']['hits'])) {
            $total = intval($data['hits']['total']);
            if (!$this->select_nested()) {
                foreach ($data['hits']['hits'] as $row) {
                    $return[] = $row['_source'];
                }
            } else {
                // 1. 将 nested 节点扁平化
                foreach ($data['hits']['hits'] as $row) {
                    $row = $row['_source'];
                    $new = $this->unpack_nested_all($row, array_keys($this->nested));
                    $return = array_merge($return, $new);
                }
                // 2. nested 过滤
                $return = $this->filter_nested($return, $this->nested);
            }

            //3. 实现 select("field as x") 的功能
            if (array_diff($this->select, array_keys($this->select))) {

                foreach ($return as $i => $row) {
                    $new_row = [];
                    foreach ($this->select as $field => $as) {
                        $field = explode(".", $field);
                        $as = explode(".", $as);
                        // 取出对应的节点值
                        $element = $this->get_element($row, $field);
                        // 写入新节点
                        $new_row = $this->put_element($new_row, $as, $element);
                    }
                    $return[$i] = $new_row;
                }
            }
        }
        return ['total' => $total, 'rows' => array_values($return)];
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

    private function build_query($query, $json_encode = true)
    {
        if ($query !== null) {
            $post_fields = $query;
        } else {
            $post_fields = $this->to_query();
        }
        if ($json_encode) {
            return json_encode($post_fields);
        } else {
            return $post_fields;
        }
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

        $head = "";
        switch ($operation) {
        case "":
            return $head . $this->index;

        case "_bulk":
        case "_aliases":
        case "_cluster":
            return $head . $operation;

        case "_mapping":
        case "_settings":
        case "_alias":
            return $head . $this->index . "/" . $operation;

        case "_count":
        case "_search":
            return $head . $path . $operation . "?" . http_build_query($get);

        case "scroll":
            return $head . "_search/scroll";
        case "scroll_search":
            return $head . $path . "_search?scroll=" . $this->scroll_maxtime;

        case "get_by_id":
            return $head . $path;
        }
    }

    private function fetch_result(array $result, $operation, $code, $equal = true)
    {
        $return = [];
        if (isset($result['error']) || !isset($result['items'])) {
            if (isset($result['error'])) {
                $this->set_error(json_encode($result['error']));
            }
            return $return;
        }
        foreach ($result['items'] as $r) {
            if (isset($r[$operation]['error'])) {
                $this->set_error(json_encode($r));
            }
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
        if (is_string($_id)) {
            $_id = [$_id];
        }

        foreach ($data as $d) {
            $row = $d;
            $d = [];
            $d['_index'] = $this->index;
            $d['_type'] = $this->type;
            $d['_id'] = $this->fetch_id_key($row, $_id);
            $body[] = json_encode([$operation => $d]);
            if ($operation == "update") {
                $body[] = json_encode(["doc" => $row]);
            } elseif ($operation == "create") {
                $body[] = json_encode($row);
            } elseif ($operation == "delete") {
                ;   //do nothing
            }
        }
        return implode("\n", $body) . "\n";
    }

    private function fetch_id_key($row, array $_id_union)
    {
        $_id = [];
        foreach ($_id_union as $key) {
            if (!isset($row[$key])) {
                throw new \Exception("unknown _id of document: '$key', row=" . json_encode($row));
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

    private function is_ok($curl_result, &$data = null)
    {
        if ($curl_result === false) {
            return false;
        }
        $res = json_decode($curl_result, true);
        if (!is_array($res)) {
            $this->set_error($curl_result);
            $data = false;
            return false;
        }
        if (isset($res['error'])) {
            $this->set_error($curl_result);
            $this->status = intval($res['status']);
            $data = false;
            return false;
        } else {
            $data = $res;
            return true;
        }
    }


    /*
     * nested functions
     */

    private function filter_nested($rows, $nested)
    {
        foreach ($rows as $i => $r) {
            foreach ($nested as $path => $rules) {
                foreach ($rules as $bool => $rule) {
                    $e = $this->get_element($r, explode(".", $path), $exists);
                    if ($exists && !$this->compare_bool($bool, $e, $rule)) {
                        unset($rows[$i]);
                    }
                }
            }
        }
        return $rows;
    }

    private function compare_should($row, $rule)
    {
        foreach ($rule as $i => $rl) {
            if ($this->compare_bool($i, $row, $rl)) {
                return true;
            }
        }
        return false;
    }

    private function compare_must($row, $rule)
    {
        foreach ($rule as $i => $rl) {
            if (!$this->compare_bool($i, $row, $rl)) {
                return false;
            }
        }
        return true;
    }


    private function compare_mustnot($row, $rule)
    {
        foreach ($rule as $i => $rl) {
            if ($this->compare_bool($i, $row, $rl)) {
                return false;
            }
        }
        return true;
    }

    private function compare_bool($bool, $row, $rule)
    {
        // int 0 跟任意字符串比较都是 true, 需要转成字符串""
        $bool = "" . $bool;

        switch ($bool) {
        case "must_not":
            return $this->compare_mustnot($row, $rule);
            break;

        case "must":
            return $this->compare_must($row, $rule);
            break;

        case "should":
            return $this->compare_should($row, $rule);
            break;

        default:
            return $this->compare($row, $rule);
            break;
        }
    }

    private function compare($row, $rule)
    {
        $path = explode(".", $rule[0]);
        $element = $this->get_element($row, $path, $exists);
        switch ($rule[1]) {
        case "=":
            return $element == $rule[2];
            break;

        case "<":
            return $element < $rule[2];
            break;

        case ">":
            return $element > $rule[2];
            break;

        case ">=":
            return $element >= $rule[2];
            break;

        case "<=":
            return $element <= $rule[2];
            break;

        case "in":
            return in_array($element, $rule[2]);
            break;

        case "exists":
            return $exists == true;
            break;

        case "!=":
            return $element != $rule[2];
            break;
        }
    }

    private function unpack_nested_all($arr, array $path)
    {
        $arr = [$arr];
        foreach ($path as $p) {
            $new = [];
            foreach ($arr as $r) {
                $new = array_merge($new, $this->unpack_nested($r, $p));
            }
            $arr = $new;
        }
        return $arr;
    }

    private function unpack_nested($arr, $path)
    {
        $path = explode(".", $path);
        // 获取 nested 节点
        $nested = $this->get_element($arr, $path);

        // 将 nested 扁平化
        $list = [];
        if ($nested) {
            foreach ($nested as $node) {
                $list[] = $this->put_element($arr, $path, $node);
            }
        } else {
            $list[] = $arr;
        }
        return $list;
    }

    private function put_element($arr, array $path, $node)
    {
        $key = array_shift($path);
        if (empty($path)) {
            $arr[$key] = $node;
        } else {
            if (!isset($arr[$key])) {
                $arr[$key] = [];
            }
            $arr[$key] = $this->put_element($arr[$key], $path, $node);
        }
        return $arr;
    }

    private function get_element($arr, array $path, &$exists = false)
    {
        if (empty($path)) {
            $exists = true;
            return $arr;
        }
        if (!is_array($arr)) {
            return null;
        }
        $key = array_shift($path);
        foreach ($arr as $k => $v) {
            if ($k == $key) {
                return $this->get_element($v, $path, $exists);
            }
        }
    }

    private function into_nested($path)
    {
        array_unshift($this->curr_nested, $path);
        $this->nested_bool[$path] = [];
    }

    private function outof_nested()
    {
        $path = array_shift($this->curr_nested);
        $this->nested_bool[$path] = [];
    }

    private function tryto_push_nested_bool($bool)
    {
        if ($this->curr_nested) {
            $path = $this->curr_nested[0];
            array_push($this->nested_bool[$path], $bool);
        }
    }

    private function tryto_pop_nested_bool()
    {
        if ($this->curr_nested) {
            $path = $this->curr_nested[0];
            array_pop($this->nested_bool[$path]);
        }
    }

    private function tryto_put_nested($field, $op, $value)
    {
        if ($this->curr_nested) {
            $path = $this->curr_nested[0];

            /*
             * 判断是否是在 nested field内
             * nested field  例如: "e.f.name"
             * nested path   例如: "e.f"
             * */
            if (strpos($field, $path) !== 0) {
                return;
            }

            $tmp = &$this->nested[$path];
            /*
             * nested_bool 例如:  ["must", "should" ...]
             * 递归找到 nested 对应的节点，写入 $op 和 $value:
             * 例如：
             * [
                    "e.f" => [
                        "must" => [
                            ["d", ">=", 2],
                            "should" => [
                                ["d", "!=", 0],
                                ["d", "<", 4],
                            ],
                            ["d", "<", 3],
                        ],
                    ],
               ]
             * */
            foreach ($this->nested_bool[$path] as $bool) {
                if (!isset($tmp[$bool])) {
                    $tmp[$bool] = [];
                }
                $tmp = &$tmp[$bool];
            }
            $sub_path = substr($field, strlen($path) + 1);
            $tmp[] = [$sub_path, $op, $value];
        }
    }
}
