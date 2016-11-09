<?php

use PHPUnit\Framework\TestCase;
require __DIR__ . "/../ES.php";
use App\Lib\ES;

class ESTestCurl extends TestCase
{
	public function test()
	{
		$es = $this->getes();
        $res = $es->drop_index();
        $this->assertEquals(true,  !empty($res));
        $res = $es->create();
        $this->assertEquals(true,  !empty($res));
		$data = [
			["id" => 1, "name" => "awen", "age" => 12],
			["id" => 2, "name" => "awen1", "age" => 13],
			["id" => 3, "name" => "awen2", "age" => 14],
			["id" => 4, "name" => "awen3", "age" => 15],
		];
		$es->bulk_upsert($data, "id");
        sleep(1);

        $es = new ES;
        $es->index("fortest");
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

        $es = new ES;
        $es->port = 9900;
        $res = $es->index("fortest")->type("fortest")->count();
        $this->assertEquals($res, false);
        $this->assertEquals(true, strpos($es->error(), "Connection refused") !== false);
	}

	public function test_upsert()
	{
		$es = $this->getes();
        $es->drop_index();
        $es->create();
		$data = [
			["id" => 1, "name" => "awen", "age" => 12],
			["id" => 2, "name" => "awen1", "age" => 13],
			["id" => 3, "name" => "awen2", "age" => 14],
			["id" => 4, "name" => "awen3", "age" => 15],
		];
		$es->bulk_upsert($data, "id");
        sleep(1);

		$row = $es->type("fortest")->select("id")->where("id", "=", 2)->search();
		$this->assertEquals($row[0], ["id" => 2]);

        $es->drop_index();
        $es->create();
		$data = [
			["id-i" => 1, "name_f" => "awen", "age" => 12],
			["id-i" => 2, "name_f" => "awen1", "age" => 13],
			["id-i" => 2, "name_f" => "awen", "age" => 14],
			["id-i" => 1, "name_f" => "awen1", "age" => 15],
		];
		$es->bulk_upsert($data, ["id-i", "name_f"]);
        sleep(1);
		$row = $es->type("fortest")->select("id-i as id_alias", "name_f as name")->where("id-i", "=", 2)->sort("age", "desc")->search();
		$this->assertEquals($row, [["id_alias" => 2, "name" => "awen"], ["id_alias" => 2, "name" => "awen1"]]);


        $es->drop_index();
        $es->create();
		$data = [
			["id" => 1, "name" => "awen", "age" => 12],
			["id" => 2, "name" => "awen1", "age" => 13],
			["id" => 2, "name" => "awen", "age" => 14],
			["id" => 1, "name" => "awen1", "age" => 15],
		];
		$es->type("fortest")->bulk_upsert($data, ["id", "name"]);
        sleep(1);

		$num = $es->type("fortest")->select("id")->where("id", "=", 2)->count();
		$this->assertEquals($num, 2);

		$num = $es->type("fortest")->select("id")->where("id", "=", 1)->count();
		$this->assertEquals($num, 2);

		$num = $es->type("fortest")->count();
		$this->assertEquals($num, 4);

		$row = $es->type("fortest")->select("_id", "_score", "_type as type", "_index as index")->where("_id", "=", "1.awen")->search();
		$this->assertEquals($row[0], ["_id" => "1.awen", "_score" => 1, "type" => "fortest", "index" => "fortest"]);

		$row = $es
			->type("fortest")
			->where("id", "=", 1)
			->where("name", "=", "awen")
			->search();

		$this->assertEquals($row[0], ["id" => 1, "name" => "awen", "age" => 12]);


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

