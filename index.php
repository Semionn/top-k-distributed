<?php

include 'config/commonConfig.php';
include 'config/dbConfig.php';
include 'src/TopKSolver.php';
include 'src/TRUT.php';
include 'src/DBTopKManager.php';
include 'src/RedisDBTopKManager.php';
use StreamCounterTask\TopKSolver;
use StreamCounterTask\TRUT;
use StreamCounterTask\RedisDBTopKManager;


$currTime = getCurrentTimeFrame(TIME_FRAME_SIZE);
$method = $_SERVER['REQUEST_METHOD'];
$request = explode('/', trim($_SERVER['PATH_INFO'],'/'));
$input = json_decode(file_get_contents('php://input'),true);

$endpoint = preg_replace('/[^a-z0-9_]+/i','',array_shift($request));
$arg = array_shift($request);

$target_func = null;

switch ($endpoint) {
    case "process":
        $target_func = function (TopKSolver $topKSolver, string $arg) use ($currTime) {
            $topKSolver->processText($arg);
            echo $currTime;
        };
        break;
    case "get":
        $target_func = function (TopKSolver $topKSolver, string $arg) {
            $res = $topKSolver->getTopK($arg);
            if ($res === false) {
                echo "No_info";
            } else {
                $out = $sep = '';
                foreach( $res as $key => $value ) {
                    $out .= $sep . $key . ':' . $value;
                    $sep = ',';
                }
                echo $out;
            }
        };
        break;
    default:
        echo "Unknown endpoint :".$endpoint."<br>";
        break;
}

if ($target_func != null) {
    # lock to avoid concurrent using of same topk-solver instance
    $localRedis = new Redis();
    $localRedis->connect(REDIS_LOCAL_IP, REDIS_LOCAL_PORT);
    $localDBManager = new RedisDBTopKManager($localRedis);
    $fullSolverKey = SOLVER_KEY.strval($currTime);
    $solverLockKey = SOLVER_LOCK_KEY.strval(getCurrentTimeFrame(TIME_FRAME_SIZE));
    $localDBManager->lock($solverLockKey, $timeout=LOCKING_TIMEOUT);

    $redis = new Redis();
    $redis->connect(REDIS_IP, REDIS_PORT);
    $dbManager = new RedisDBTopKManager($redis);
    $dbSolver = $localRedis->get($fullSolverKey);
    if ($dbSolver === false) {
        $topKSolver = new TRUT($dbManager, TOP_K, TIME_FRAME_SIZE);
    } else {
        $topKSolver = unserialize($dbSolver);
        $topKSolver->setDBTopKManager($dbManager);
    }
    $target_func($topKSolver, $arg);
    $serialized = serialize($topKSolver);
    $localRedis->set($fullSolverKey, $serialized);

    $localDBManager->unlock($solverLockKey);
}
