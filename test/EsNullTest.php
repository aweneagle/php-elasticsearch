<?php
use PHPUnit\Framework\TestCase;
class EsTestNull extends TestCase
{
    private static $es = null;

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
                "null" => "nested",
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
                    ["d" => 3],
                ],
            ],
        ];
        $es->bulk_upsert([$a], "id");
        $es->index("fortest")->type("fortest");
        sleep(1);
        return $es;
    }

    public function testExistsFunc()
    {
        /* exists 应该计入到 nested 字段的获取中 */
        $res = $this->es()
            ->select("b.c as name")
            ->nested("b", function($es) {
                $es->should(function($es) {
                    $es->exists("b.c", true);   //true
                    $es->where("b.c", ">", 5);  //false
                });
            })
            ->search();
        $this->assertSame($res['rows'], [["name" => 2], ["name" => 3]]);

        /* exists 应该计入到 nested 字段的获取中 */
        $res = $this->es()
            ->select("b.c as name")
            ->nested("b", function($es) {
                $es->should(function($es) {
                    $es->exists("b.d", true);   //false
                    $es->where("b.c", "<", 5);  //true
                });
            })
            ->search();
        $this->assertSame($res['rows'], [["name" => 2], ["name" => 3]]);

        /* exists 应该计入到 nested 字段的获取中 */
        $res = $this->es()
            ->select("b.c as name")
            ->nested("b", function($es) {
                $es->should(function($es) {
                    $es->where("b.d", "<", 5);  //false
                    $es->exists("b.c", true);   //true
                });
            })
            ->search();
        $this->assertSame($res['rows'], [["name" => 2], ["name" => 3]]);

        /* exists 应该计入到 nested 字段的获取中 */
        $res = $this->es()
            ->select("b.c as name")
            ->nested("b", function($es) {
                $es->should(function($es) {
                    $es->where("b.d", "<", 5);  //false
                    $es->exists("b.d", false);   //true
                });
            })
            ->search();
        $this->assertSame($res['rows'], [["name" => 2], ["name" => 3]]);

        /* exists 应该计入到 nested 字段的获取中 */
        $res = $this->es()
            ->select("b.c as name")
            ->nested("b", function($es) {
                $es->should(function($es) {
                    $es->where("b.d", "=", 0);  //false, b.d not exists
                });
            })
            ->search();
        $this->assertSame($res['rows'], []);
    }

    public function testNullSelectNested()
    {
        /* select 的字段不存在, nested 不符合， 但 rows 仍然是应该返回的 */
        $res = $this->es()
            ->select("a as name", "null.c as Null")
            ->should(function($es) {
                $es->where("a", "=", 1);
                $es->nested("null", function($es) {
                    $es->must(function($es) {
                        $es->where("null.c", ">=", 2);
                    });
                });
            })
            ->search();
        $this->assertSame([["name" => 1, "Null" => null]], $res['rows']);
        $this->assertSame(1, $res['total']);

    }

    public function testNullSelect()
    {
        /* select 的字段不存在, nested 符合 */
        $res = $this->es()
            ->select("a as name", "null.f.g")
            ->should(function($es) {
                $es->where("a", "=", 1);
                $es->nested("e.f", function($es) {
                    $es->must(function($es) {
                        $es->where("e.f.g", ">=", 2);
                    });
                });
            })
            ->search();
        $this->assertSame([["name" => 1, "null" =>["f" => ["g" => null]]]], $res['rows']);
        $this->assertSame(1, $res['total']);

    }


    public function testNested()
    {
        $es = $this->es();
        $es->nested("e.f", function($es) {
            $es->must(function($es) {
                $es->where("e.f.d", ">=", 2);
                $es->should(function($es){
                    $es->where("e.f.g", "=", 0);
                    $es->where("e.f.g", "<", 4);
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
                $es->where("e.f.g", "=", 0);
                $es->where("e.f.g", "<", 4);
            });
        });
        $result = $es->select("id as ID", "a as name", "e.f.g as element")->search();
        $cmp = [
                ["ID" => 1, "name" => 1, "element" => 2],
                ["ID" => 1, "name" => 1, "element" => null],
            ];
        $this->assertSame($cmp, $result['rows']);
    }

    public function testSelectNoNestedAs()
    {
        $res = $this->es()
            ->select("a as name")
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.g", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([["name" => 1]], $res['rows']);
        $this->assertSame(1, $res['total']);

    }


    public function testSelectAll()
    {
        $es = $this->es();
        $es->nested("e.f", function($es) {
            $es->must(function($es) {
                $es->where("e.f.g", ">=", 2);
            });
        });
        $row = [
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
                    ["d" => 3],
                ],
            ],
        ];
        $res = $es->search();
        $this->assertSame([$row], $res['rows']);
        $this->assertSame(1, $res['total']);
    }

    public function testSelectNestedAs()
    {
        $res = $this->es()
            ->select("e.f.d as gold")
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.g", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([["gold" => 2]], $res['rows']);
        $this->assertSame(1, $res['total']);

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
            ["silver" => 2, "gold"=>null],
            ["silver" => 3, "gold"=>2], 
            ["silver" => 3, "gold"=>null],
        ], $res['rows']);
        $this->assertSame(1, $res['total']);

        $res = $this->es()
            ->select("e.f.g as gold")
            ->nested("e.f", function($es) {
                $es->must(function($es) {
                    $es->where("e.f.d", ">=", 2);
                });
            })
            ->search();
        $this->assertSame([
            ["gold"=>2], 
            ["gold"=>null],
        ], $res['rows']);
        $this->assertSame(1, $res['total']);

    }
}
