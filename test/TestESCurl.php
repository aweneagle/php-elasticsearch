<?php

use PHPUnit\Framework\TestCase;
require __DIR__ . "/../ES.php";
use App\Lib\ES;

class ESTestCurl extends TestCase
{
	public function test()
	{
		$es = $this->getes();
		$data = [
			["id" => 1, "name" => "awen", "age" => 12],
			["id" => 2, "name" => "awen1", "age" => 13],
			["id" => 3, "name" => "awen2", "age" => 14],
			["id" => 4, "name" => "awen3", "age" => 15],
		];
		$es->bulk_upsert($data, "id");

		$row = $es->type("fortest")->select("id")->where("id", "=", 2)->search();
		$this->assertEquals($row[0], ["id" => 2]);

		$row = $es->type("fortest")->select("id")->where("id_null", "=", 2)->search();
		$this->assertEquals($row, []);

		$row = $es->type("fortest")->select("id_null")->where("id", "=", 2)->search();
		$this->assertEquals($row[0], ["id_null" => false]);

		$row = $es->type("fortest")->select("id_null")->where("id_null", "=", 2)->search();
		$this->assertEquals($row, []);

		$row = $es->type("fortest")->select("id", "name")->where("id", "=", 2)->search();
		$this->assertEquals($row[0], ["id" => 2, "name" => "awen1"]);

		$row = $es->type("fortest")->select("id_null", "name")->where("id", "=", 2)->search();
		$this->assertEquals($row[0], ["id_null" => false, "name" => "awen1"]);

		$row = $es->type("fortest")->select("id", "name")->where("id_null", "=", 2)->search();
		$this->assertEquals($row, []);

		$row = $es->type("fortest")->select("id_null", "name")->where("id_null", "=", 2)->search();
		$this->assertEquals($row, []);

		$row = $es->type("fortest")->where("id", "=", 2)->search();
		$this->assertEquals($row[0], ["id" => 2, "name" => "awen1", "age" => 13]);

	}

	public function test_upsert()
	{
		$es = $this->getes();
		$data = [
			["id" => 1, "name" => "awen", "age" => 12],
			["id" => 2, "name" => "awen1", "age" => 13],
			["id" => 3, "name" => "awen2", "age" => 14],
			["id" => 4, "name" => "awen3", "age" => 15],
		];
		$es->bulk_upsert($data, "id");

		$row = $es->type("fortest")->select("id")->where("id", "=", 2)->search();
		$this->assertEquals($row[0], ["id" => 2]);

		$data = [
			["id" => 1, "name" => "awen", "age" => 12],
			["id" => 2, "name" => "awen1", "age" => 13],
			["id" => 2, "name" => "awen", "age" => 14],
			["id" => 1, "name" => "awen1", "age" => 15],
		];
		$es->type("fortest2")->bulk_upsert($data, ["id", "name"]);

		$num = $es->type("fortest")->select("id")->where("id", "=", 2)->count();
		$this->assertEquals($num, 2);

		$num = $es->type("fortest")->select("id")->where("id", "=", 1)->count();
		$this->assertEquals($num, 2);

		$num = $es->type("fortest")->count();
		$this->assertEquals($num, 4);

		$row = $es
			->type("fortest")
			->select("_id")
			->where("id", "=", 1)
			->where("name", "=", "awen")
			->search();

		$this->assertEquals($row[0]["_id"], "1.awen");

	}

	private function getes()
	{
		$es = new ES;
		$es->index("fortest")->type("fortest");
		return $es;
	}
}

