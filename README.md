# php-es
ES, ElasticSearch 

a simple tool for elasticsearch operation

使用示例：

1. 查询 商店中作者 "king" 和 "bible" 两人价格为 50~100的
含有"Elasticsearch" 的书

```php
$es = new ES;
$es->index("store")
->type("books")
->must(function(ES $es) {
		$es->where("author", "in", ["king", "bible"]);
		$es->must(function(ES $es) {
			$es->where("price", "<=", 100);
			$es->where("price", ">=", 50);
			});
		})
->should(function(ES $es) {
		$es->match("title", "Elasticsearch", 2);    //标题最重要
		$es->match("subtitle", "Elasticsearch");    //子标题其次
		$es->should(function(ES $es) {
			$es->match("desc", "Elasticsearch");    //描述 和 内容没那么重要
			$es->match("content", "Elasticsearch");
			});
		})
->sort_score()             //按照内容重要性排序
	->sort('date', 'desc')    //再按发布时间排序
	->search();               //查询数据
```

2. 统计 商店中作者名不包含"king"的书的数量

```php
$es = new ES;
$es->index("store")
->type("books")
->must_not(function(ES $es) {
	$es->where("author", "=", "king");
})
->count();
```

3. 批量update 和 insert 商店中的书籍信息
```php
// 'id' is a unique key
$data = [
	["id" => 1, "name" => "Elastic Search 中文指南", "author" => "king"],
	["id" => 2, "name" => "Elastic Search 进阶", "author" => "adom"],
	["id" => 3, "name" => "精通 Elastic", "author" => "adom"],
];
$es = new ES;
$es->index("store")
->type("books")
->bulk_upsert($data, "id");
```

4. 批量update 和 insert 商店中的书籍信息（组合_id键）
```php
// 'id','store_id' 一起成为一个唯一键
$data = [
	["id" => 1, "store_id"=>1, "name" => "Elastic Search 中文指南", "author" => "king"],
	["id" => 1, "store_id"=>2, "name" => "Elastic Search 进阶", "author" => "adom"],
	["id" => 2, "store_id"=>1, "name" => "精通 Elastic", "author" => "adom"],
];
$es = new ES;
$es->index("store)
->type("books")
->bulk_upsert($data, ["id", "store_id"]);
```

5. 获取商店中所有书名
```php
$es = new ES;
$es->index("store")
->type("books")
->select("name as book_name")
->search();
```

6. nested query
```php
$es = new ES;
$data = [
	[
		"id" => 1, 
		"tags" => [
			["name" => "tech", "type" => 1],
			["name" => "manual", "type" => 2],
			["name" => "chinese", "type" => 3],
		], 
		"name" => "Elasticsearch 中文指南",
	],
	[
		"id" => 2, 
		"tags" => [
			["name" => "tech", "type" => 1],
			["name" => "manual", "type" => 2],
			["name" => "english", "type" => 3],
		], 
		"name" => "Elasticsearch manual",
	],
];
$es->index("store")->type("books")
->bulk_upsert($data, "id");

$es->nested("tags", function($es) {
	$es->where("tags.name", "=", "tech");
	$es->where("tags.type", "=", "2");
});

7. 使用别名功能
```php
$es = new ES;
$es->index("fortest_v1")->alias("fortest");  //给fortest_v1创建别名fortest
$es->index("fortest")->re_alias("fortest_v1", "fortest_v2"); //将别名fortest 从 fortest_v1 移除，指向 fortest_v2
