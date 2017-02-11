<?php

namespace StreamCounterTask;

include 'RedLock.php';
use Redis;

class RedisDBTopKManager implements DBTopKManager
{
    private $redisObj;
    private $locks = [];

    function __construct(Redis $redisObj) {
        $this->redisObj = $redisObj;
    }

    public function lock($key, $timeout = 5 * 60) {
        if (array_key_exists($key, $this->locks)) {
            $this->unlock($key);
        }
        $lock = new RedLock($this->redisObj, $timeout);
        $locked = $lock->lock($key, $timeout); // for simplicity set ttl equal to timeout
        if ($locked) {
            $this->locks[$key] = $lock;
        }
        return $locked;
    }

    public function tryLock($key) {
        $res = $this->lock($key, 5);
        return $res;
    }

    public function unlock($key){
        if (array_key_exists($key,$this->locks)) {
            $lock = $this->locks[$key];
            $lock->unlock();
            unset($this->locks[$key]);
        }
    }

    public function incrKey($key, $amount = 1) {
        $this->redisObj->incrBy($key, $amount);
    }

    public function getMap($key, $value_mapper) {
        $temp = $this->redisObj->hgetall($key);
        $result = array();
        while (list($key, $val) = each($temp)) {
            $result[$key] = $value_mapper($val);
        }
        return $result;
    }

    public function setMap($key, $hmap) {
        $this->redisObj->hmset($key, $hmap);
    }

    public function storeByKey($key, $value) {
        return $this->redisObj->set($key, $value);
    }

    public function getByKey($key) {
        return $this->redisObj->get($key);
    }

    public function keyExists($key) {
        return $this->redisObj->exists($key);
    }
}
