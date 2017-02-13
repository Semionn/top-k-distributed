<?php

namespace StreamCounterTask;

include 'RedLock.php';
use Redis;

//TODO: add pipe support
class RedisDBTopKManager implements DBTopKManager
{
    /** @var Redis */
    private $redisObj;

    /** @var array */
    private $locks = array();

    function __construct(Redis $redisObj) {
        $this->redisObj = $redisObj;
    }

    public function lock(string $key, int $timeout = 5 * 60) {
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

    public function tryLock(string $key): bool {
        $res = $this->lock($key, 5);
        return $res;
    }

    public function unlock(string $key){
        if (array_key_exists($key,$this->locks)) {
            $lock = $this->locks[$key];
            $lock->unlock();
            unset($this->locks[$key]);
        }
    }

    public function incrKey(string $key, int $amount = 1) {
        $this->redisObj->incrBy($key, $amount);
    }

    public function getMap(string $key, $value_mapper): array {
        $temp = $this->redisObj->hgetall($key);
        $result = array();
        while (list($key, $val) = each($temp)) {
            $result[$key] = $value_mapper($val);
        }
        return $result;
    }

    public function setMap(string $key, array $hmap) {
        $this->redisObj->hmset($key, $hmap);
    }

    public function storeByKey(string $key, string $value) {
        return $this->redisObj->set($key, $value);
    }

    public function getByKey(string $key): string {
        return $this->redisObj->get($key);
    }

    public function keyExists(string $key): bool {
        return $this->redisObj->exists($key);
    }

    public function deleteKeys(array $keys) {
        foreach ($keys as $key) {
            $this->redisObj->delete($key);
        }
    }

    public function delete(string $key) {
        $this->redisObj->delete($key);
    }

    public function lenByKey(string $key): int
    {
        return $this->redisObj->llen($key);
    }

    public function getByIndex(string $key, int $index): string
    {
        return $this->redisObj->lIndex($key, $index);
    }

    public function setByHash(string $key, string $hashKey, string $store)
    {
        $this->redisObj->hset($key, $hashKey, $store);
    }

    public function pushRightByKey(string $key, string $value)
    {
        $this->redisObj->rpush($key, $value);
    }
}
