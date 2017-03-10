<?php
error_reporting(E_ALL);
require __DIR__ . "/../Lib/ES.php";
require __DIR__ . "/../Lib/AutoQconf.php";
require __DIR__ . "/../Lib/Alarm.php";
require __DIR__ . "/../Lib/Conf.php";
require __DIR__ . "/../Lib/Debug.php";

use Tools\ES;

class GlobalTest {
    public static function initEs()
    {
        $es = new ES;
        $es->set_cluster([["host" => "127.0.0.1", "port"=>9200, "user" => '', "pass" => '']]);
        return $es->index("fortest")->type("fortest");
    }

    public static function initClusterEs()
    {
        $es = new ES;
        $es->set_cluster([
            [
                "host" => "127.0.0.1",
                "port" => "9990",
                "user" => "",
                "pass" => "",
            ],
            [
                "host" => "127.0.0.1",
                "port" => "9200",
                "user" => "",
                "pass" => "",
            ],
        ]);
        return $es;
    }
}
