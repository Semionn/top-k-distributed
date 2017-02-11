<?php
chdir(__DIR__);

include '../config/commonConfig.php';
include '../config/dbConfig.php';
include 'TopKSolver.php';
include 'TRUT.php';
include 'DBTopKManager.php';
include 'RedisDBTopKManager.php';
use StreamCounterTask\RedisDBTopKManager;

$curTime = getCurrentTimeFrame(TIME_FRAME_SIZE);
$lastTime = $curTime - TIME_FRAME_SIZE;
$lastSolverKey = SOLVER_KEY.strval($lastTime);

$localRedis = new Redis();
$localRedis->connect(REDIS_LOCAL_IP, REDIS_LOCAL_PORT);

$redis = new Redis();
$redis->connect(REDIS_IP, REDIS_PORT);
$dbManager = new RedisDBTopKManager($redis);

$dbSolver = $localRedis->get($lastSolverKey);
if ($dbSolver !== false) {
    $topKSolver = unserialize($dbSolver);
    $topKSolver->setDBTopKManager($dbManager);
    $topKSolver->startSync($lastTime);
    $localRedis->delete($lastSolverKey);
    echo "Synced $lastTime\n";
} else {
    echo "No action $lastTime\n";
}

$dbKeys = $localRedis->keys(SOLVER_KEY."*");
if ($dbKeys !== false) {
    $curSolverKey = SOLVER_KEY.strval($curTime);
    if(($key = array_search($curSolverKey, $dbKeys)) !== false) {
        unset($dbKeys[$key]);
    }
    foreach ($dbKeys as $key) {
        $localRedis->delete($key);
    }
}