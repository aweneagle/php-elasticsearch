<?php
use PHPUnit\Framework\TestCase;
class EsTest extends TestCase
{
    private static $es;
    private static $row;

    private function es()
    {
        if (self::$es) {
            return self::$es->index("fortest")->type("fortest");
        }
        $es = GlobalTest::initEs()->index("fortest")->type("fortest");
        self::$es = $es;
        $es->delete();
        $res = $es->create([
            "number_of_shards" => 1,
        ]);
        $this->assertSame($res !== false, true);
        $res = $es->mapping([
            "fortest" => [
                "id" => "long",
                "a" => "long",
                "b" => "nested",
                "b.c" => "long",
                "e.f" => "nested",
                "e.f.d" => "long",
            ]
        ]);
        $this->assertSame($res !== false, true);

        $a = [
            "id" => 1,
            "a" => 1,
            "b" => [        //nested node
                ["c" => 2],
                ["c" => 3],
            ],
            "e" => [
                "f" => [    //nested node
                    ["d" => 1, "g" => 1],
                    ["d" => 2, "g" => 2],
                    ["d" => 3, "g" => 1],
                ],
            ],
        ];
        self::$row = $a;
        $es->bulk_upsert([$a], "id");
        $es->index("fortest")->type("fortest");
        sleep(2);
        return $es;
    }

    public function testSelectNested()
    {
        $res = $this->es()
            ->select("b.c as silver", "e.f.g as gold")
            ->nested("b", function($es) {
                $es->must(function($es) {
                    $es->where("b.c", ">=", 2);
                });
            })
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.d", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([
            ["silver" => 2, "gold"=>2], 
            ["silver" => 2, "gold"=>1],
            ["silver" => 3, "gold"=>2], 
            ["silver" => 3, "gold"=>1],
        ], $res['rows']);
        $this->assertSame(1, $res['total']);

        $res = $this->es()
            ->select("e.f.d as gold")
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.d", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([
            ["gold"=>2], 
            ["gold"=>3],
        ], $res['rows']);
        $this->assertSame(1, $res['total']);

    }

    public function testSelectNestedAs()
    {
        $res = $this->es()
            ->select("e.f.g as gold")
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.d", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([["gold" => 2], ["gold" => 1]], $res['rows']);
        $this->assertSame(1, $res['total']);

    }


    public function testSelectAll()
    {
        $es = $this->es();
        $es->nested("e.f", function($es) {
            $es->must(function($es) {
                $es->where("e.f.d", ">=", 2);
            });
        });
        $res = $es->search();
        $this->assertSame([self::$row], $res['rows']);
        $this->assertSame(1, $res['total']);
    }

    public function testSelectNoNestedAs()
    {
        $res = $this->es()
            ->select("a as name")
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.d", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([["name" => 1]], $res['rows']);
        $this->assertSame(1, $res['total']);

    }


    public function testSelectNoNested()
    {
        $res = $this->es()
            ->select("a")
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.d", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([["a" => 1]], $res['rows']);
        $this->assertSame(1, $res['total']);

    }

    public function testNested()
    {
        $es = $this->es();
        $es->nested("e.f", function($es) {
            $es->must(function($es) {
                $es->where("e.f.d", ">=", 2);
                $es->should(function($es){
                    $es->where("e.f.d", "=", 0);
                    $es->where("e.f.d", "<", 4);
                });
            });
        });
        $result = $es->select("id as ID", "a as name", "e.f.d as element")->search();
        $cmp = [
                ["ID" => 1, "name" => 1, "element" => 2],
                ["ID" => 1, "name" => 1, "element" => 3],
            ];
        $this->assertSame($cmp, $result['rows']);

        // without must() wrapper
        $es->index("fortest")
            ->type("fortest");

        $es->nested("e.f", function($es) {
            $es->where("e.f.d", ">=", 2);
            $es->should(function($es){
                $es->where("e.f.d", "=", 0);
                $es->where("e.f.d", "<", 4);
            });
        });
        $result = $es->select("id as ID", "a as name", "e.f.d as element")->search();
        $cmp = [
                ["ID" => 1, "name" => 1, "element" => 2],
                ["ID" => 1, "name" => 1, "element" => 3],
            ];
        $this->assertSame($cmp, $result['rows']);
    }

    public function testCluster()
    {
        $es = GlobalTest::initClusterEs();
        $es->index("fortest")->type("fortest");

        // delete() 不能走 cluster
        $res = $es->delete();
        $this->assertSame($res === false, true);

        // create() 不能走 cluster
        $res = $es->create([
            "number_of_shards" => 1,
        ]);
        $this->assertSame($res === false, true);

        // count() 走 cluster
        $res = $es->count();
        $this->assertSame($res, 1);

        // search() 走 cluster
        $res = $es->search();
        $this->assertSame(count($res['rows']), 1);

        // aggs() 走 cluster
        $res = $es->aggs(["a" => "sum"]);
        $this->assertSame($res, ['a' => 1.0]);

    }
}
