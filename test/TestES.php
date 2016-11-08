<?php
use PHPUnit\Framework\TestCase;
require __DIR__ . "/../ES.php";
use App\Lib\ES;
class ESTest extends TestCase
{
	public function testDefault()
	{
        $es = new ES;
		$query = $es->to_query();
		$this->assertEquals($query, [
			"query" => [
				"match_all" => []
			]
		]);

        /* where 默认是在 must 子句中 */
        $es = new ES;
        $es->where("price", ">=", 10);
        $query1 = $es->to_query();

        $es = new ES;
        $es->must(function($es) {
            $es->where("price", ">=", 10);
        });
        $query2 = $es->to_query();
        $this->assertEquals($query1, $query2);

        /* match 默认是在should子句中 */
        $es = new ES;
        $es->match("name", "es");
        $query1 = $es->to_query();

        $es = new ES;
        $es->should(function($es) {
            $es->match("name", "es");
        });
        $query2 = $es->to_query();
        $this->assertEquals($query1, $query2);

        /* where 和 match 并存*/
        $es = new ES;
		$es->where("age", ">=", 2);
        $es->match("name", "es");
        $query1 = $es->to_query();

        $es = new ES;
        $es->must(function($es) {
            $es->where("age", ">=", 2);
        });
        $es->should(function($es) {
            $es->match("name", "es");
        });
        $query2 = $es->to_query();
        $this->assertEquals($query1, $query2);


        /* where 和 match 并存*/
        $es = new ES;
        $es->match("name", "es")
		   ->where("age", ">=", 2);
        $query1 = $es->to_query();

        $es = new ES;
        $es->should(function($es) {
            $es->match("name", "es");
        });
        $es->must(function($es) {
            $es->where("age", ">=", 2);
        });
        $query2 = $es->to_query();
        $this->assertEquals($query1, $query2);


        /* where 和 match 并存*/
        $es = new ES;
        $es->where("price", ">=", 10)
           ->match("name", "es")
		   ->where("age", ">=", 2);
        $query1 = $es->to_query();

        $es = new ES;
        $es->must(function($es) {
            $es->where("price", ">=", 10);
        });
        $es->should(function($es) {
            $es->match("name", "es");
        });
        $es->must(function($es) {
            $es->where("age", ">=", 2);
        });
        $query2 = $es->to_query();
        $this->assertEquals($query1, $query2);

        /* where 和 match 并存*/
        $es = new ES;
        $es->match("name", "es")
           ->where("age", ">", 10)
		   ->match("title", "search");
        $query1 = $es->to_query();

        $es = new ES;
        $es->should(function($es) {
            $es->match("name", "es");
            $es->match("title", "search");
        });
        $es->must(function($es) {
            $es->where("age", ">", 10);
        });
        $query2 = $es->to_query();
        $this->assertEquals($query1, $query2);

	}

	public function testMergeBool()
	{
		$this->mergeBool("must");
		$this->mergeBool("must_not");
		$this->mergeBool("should");
	}
	
    public function testShould()
    {
        $this->boolTest("should");
        $this->filterAndQueryBoolTest("should");
    }

    public function testMust()
    {
        $this->boolTest("must");
        $this->filterAndQueryBoolTest("must");

    }

    public function testMustNot()
    {
        $this->boolTest("must_not");
        $this->filterAndQueryBoolTest("must_not");
    }

    public function testSort()
    {
        $es = new ES;
        $es->must(function($es) {
            $es->where("price", ">=", 10);
        })->sort("date")
          ->sort_score()
          ->sort("age", "asc");
        $query = $es->to_query();
        $this->assertEquals($query['sort'],
            [
                ["date" => ["order" => "desc"]],
                ["_score" => ["order" => "desc"]],
                ["age" => ["order" => "asc"]],
            ]);
    }

    public function testQuery()
    {
        $this->queryBoolTest("should", "must", "should");
        $this->queryBoolTest("should", "must", "must");
        $this->queryBoolTest("should", "must", "must_not");

        $this->queryBoolTest("must", "should", "should");
        $this->queryBoolTest("must", "should", "must");
        $this->queryBoolTest("must", "should", "must_not");

        $this->queryBoolTest("must_not", "should", "should");
        $this->queryBoolTest("must_not", "should", "must");
        $this->queryBoolTest("must_not", "should", "must_not");

        $this->queryBoolTest("must_not", "must", "should");
        $this->queryBoolTest("must_not", "must", "must");
        $this->queryBoolTest("must_not", "must", "must_not");
    }

    public function testFilter()
    {
        $this->filterBoolTest("should", "must", "should");
        $this->filterBoolTest("should", "must", "must");
        $this->filterBoolTest("should", "must", "must_not");

        $this->filterBoolTest("must", "should", "should");
        $this->filterBoolTest("must", "should", "must");
        $this->filterBoolTest("must", "should", "must_not");

        $this->filterBoolTest("must_not", "should", "should");
        $this->filterBoolTest("must_not", "should", "must");
        $this->filterBoolTest("must_not", "should", "must_not");

        $this->filterBoolTest("must_not", "must", "should");
        $this->filterBoolTest("must_not", "must", "must");
        $this->filterBoolTest("must_not", "must", "must_not");
    }

    private function boolTest($bool)
    {
        $es = new ES;
        $es->$bool(function($es) {
            $es->match("title", "es");
            $es->match("content", "es");
        });
        $query = $es->to_query();
        $this->assertEquals($query, [
            "query" => [
                "bool" => [
                    "$bool" => [
                        ["match" => ["title" => "es"]],
                        ["match" => ["content" => "es"]]
                    ]
                ]
            ],
        ]);
    }

    private function queryBoolTest($bool1, $bool2, $bool3)
    {
        $es = new ES;
        $es->$bool1(function($es) {
            $es->match("title", "es", 2);
            $es->match("content", "es", 1);
        })->$bool2(function($es) use ($bool3) {
            $es->match("title", "es", 2);
            $es->$bool3(function($es) {
                $es->match("title", "es", 2);
                $es->match("content", "es", 1);
            });
            $es->match("content", "es", 1);
        });
        $this->assertEquals($es->to_query(),
            [
                "query" => [
                    "bool" => [
                        "$bool1" => [
                            ["match" => ["title" => ["query" => "es", "boost" => 2]]],
                            ["match" => ["content" => "es"]],
                        ],
                        "$bool2" => [
                            ["match" => ["title" => ["query" => "es", "boost" => 2]]],
                            ["bool" => [
                                "$bool3" => [
                                    ["match" => ["title" => ["query" => "es", "boost" => 2]]],
                                    ["match" => ["content" => "es"]],
                                ],
                            ]],
                            ["match" => ["content" => "es"]],
                        ],
                    ]
                ]
            ]
        );
    }

    private function filterBoolTest($bool1, $bool2, $bool3)
    {
        $es = new ES;
        $es->$bool1(function($es) use ($bool3) {
            $es->where("price", ">=", 2);
            $es->where("price", "<=", 10);
            $es->$bool3(function($es) {
                $es->where("author", "in", ["awen", "king"]);
                $es->where("publisher", "in", ["bbc", "acc"]);
            });
            $es->where("age", ">", 5);
            $es->where("age", "<", 9);
        });
        $es->$bool2(function($es) {
            $es->exists("weight");
            $es->exists("name");
            $es->exists("address", false);
        });
        $this->assertEquals($es->to_query(),
            [
                "filter" => [
                    "bool" => [
                        "$bool1" => [
                            ["range" => ["price" => ["gte" => 2]]],
                            ["range" => ["price" => ["lte" => 10]]],
                            ["bool" => [
                                "$bool3" => [
                                    ["terms" => ["author" => ["awen", "king"]]],
                                    ["terms" => ["publisher" => ["bbc", "acc"]]],
                                ],
                            ]],
                            ["range" => ["age" => ["gt" => 5]]],
                            ["range" => ["age" => ["lt" => 9]]],
                        ],
                        "$bool2" => [
                            ["exists" => ["field" => "weight"]],
                            ["exists" => ["field" => "name"]],
                            ["missing" => ["field" => "address"]],
                        ],
                    ]
                ]
            ]
        );
    }

    private function filterAndQueryBoolTest($bool)
    {
        $es = new ES;
        $es->$bool(function($es) {
            $es->match("title", "es");
            $es->match("content", "es");
        })->$bool(function($es) {
            $es->where("price", ">=", 10);
            $es->where("price", "<=", 100);
            $es->where("autho", "in", ["king", "awen"]);
        })->sort("date")
          ->sort("_score", "asc");

        $query = $es->to_query();
		$compare = [
            "query" => [
                "filtered" => [
                    "query" => [
                        "bool" => [
                            "$bool" => [
                                ["match" => ["title" => "es"]],
                                ["match" => ["content" => "es"]],
                            ],
                        ],
                    ],
                    "filter" => [
                        "bool" => [
                            "$bool" => [
                                ["range" => ["price" => ["gte" => 10]]],
                                ["range" => ["price" => ["lte" => 100]]],
                                ["terms" => ["autho" => ["king", "awen"]]],
                            ]
                        ]
                    ],
                ]
            ],
            "sort" => [
                ["date" => ["order" => "desc"]],
                ["_score" => ["order" => "asc"]],
            ],
        ];
        $this->assertEquals($query, $compare);

    }

	private function mergeBool($bool)
	{
		$es = new ES;
		$es->$bool(function($es){
			$es->where("age", ">=", 12);
			$es->where("age", "<=", 22);
		});
		$es->$bool(function($es){
			$es->where("price", ">", 12);
			$es->where("price", "<", 22);
		});
		$compare = [
			"filter" => [
				"bool" => [
					"$bool" => [
						["range" => ["age" => ["gte" => 12]]],
						["range" => ["age" => ["lte" => 22]]],
						["range" => ["price" => ["gt" => 12]]],
						["range" => ["price" => ["lt" => 22]]],
					],
				],
			],
		];
		$this->assertEquals($es->to_query(), $compare);
	}


}
