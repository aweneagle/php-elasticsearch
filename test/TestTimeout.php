<?php

use PHPUnit\Framework\TestCase;
require __DIR__ . "/../ES.php";
use App\Lib\ES;
class TestTimeout extends TestCase
{
	public function test()
	{
		$es = new ES;
		$data = [
			["id" => 1, "name" => "awen", "age" => 12],
			["id" => 2, "name" => "awen1", "age" => 13],
			["id" => 3, "name" => "awen2", "age" => 14],
			["id" => 4, "name" => "awen3", "age" => 15],
		];
		// prepare data
		$es->index("fortest")->type("fortest")->bulk_upsert($data, "id");
		$es->bulk_upsert($data, "id");

		$this->delay($es, 0, 1);

		$es = new ES;
		$es->port = "9999"; //a port not opened
		$this->delay($es, 0, 1);

		$es = new ES;
		$es->host = "128.2.1.1"; //a host unreachable
		$this->delay($es, 0, 1);

		$es = new ES;
		$es->host = "www.unknown_elasticsearch.com"; //unknown host
		$es->timeout_ms = 100;
		$this->delay($es, 0, 1);

		/*
		$es = new ES;
		$es->host = "www.unknown_elasticsearch.com"; //unknown host
		$es->timeout_ms = 3000;
		$this->delay($es, 2, 4);
		 */

		$es = new ES;
		$es->host = "www.unknown_elasticsearch.com"; //unknown host
		$es->conntimeout_ms = 100;
		$this->delay($es, 0, 1);

		/*
		$es = new ES;
		$es->host = "www.unknown_elasticsearch.com"; //unknown host
		$this->delay($es, 30, 95);
		 */

	}

	private function delay($es, $min_seconds, $max_seconds)
	{
		$begin = microtime(true);
		$es->index("fortest")->type("fortest")->count();
		$end = microtime(true);
		$this->assertEquals((($end - $begin) / 1000000) >= $min_seconds, true);
		$this->assertEquals((($end - $begin) / 1000000) <= $max_seconds, true);
	}
}
