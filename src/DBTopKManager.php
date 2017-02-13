<?php

namespace StreamCounterTask;


interface DBTopKManager
{
    public function lock(string $key, int $timeout = 5 * 60);

    public function unlock(string $key);

    /**
     * @param string $key
     * @param \Closure|string $value_mapper
     * @return array
     */
    public function getMap(string $key, $value_mapper): array;

    public function setMap(string $key, array $hmap);

    public function storeByKey(string $key, string $value);

    public function tryLock(string $key): bool;

    public function incrKey(string $key, int $amount = 1);

    public function getByKey(string $key): string;

    public function keyExists(string $key): bool;

    public function deleteKeys(array $keys);

    public function delete(string $key);

    public function lenByKey(string $key): int;

    public function getByIndex(string $key, int $index): string;

    public function setByHash(string $key, string $hashKey, string $store);

    public function pushRightByKey(string $key, string $value);
}
