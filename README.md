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
// 'id','store_id' 一起成为一个为意见
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
