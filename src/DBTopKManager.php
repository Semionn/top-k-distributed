<?php

namespace StreamCounterTask;


interface DBTopKManager
{
    public function lock($key, $timeout = 5*60);

    public function unlock($key);

    public function getMap($key, $value_mapper);

    public function setMap($key, $hmap);

    public function storeByKey($key, $value);

    public function tryLock($key);

    public function incrKey($key, $amount = 1);

    public function getByKey($key);

    public function keyExists($key);
}
