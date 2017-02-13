<?php

namespace StreamCounterTask;

/**
 * Interface for distributed storing data for the Top-K task
 * @package StreamCounterTask
 */
interface DBTopKManager
{
    /**
     * Trying to acquires a lock with $key until $timeout exceeds
     * TTL of lock := $timeout
     * @param string $key
     * @param int $timeout
     * @return bool is lock successful
     */
    public function lock(string $key, int $timeout = 5 * 60): bool;

    /**
     * Unlocks previously locked $key
     * @param string $key
     */
    public function unlock(string $key);

    /**
     * Returns associative array, stored at $key
     * @param string $key
     * @param \Closure|string $value_mapper
     * @return array
     */
    public function getMap(string $key, $value_mapper): array;

    /**
     * Stores associative array at $key
     * @param string $key
     * @param array $hmap
     * @return array
     */
    public function setMap(string $key, array $hmap);

    /**
     * Stores $value by $key
     * @param string $key
     * @param string $value
     */
    public function storeByKey(string $key, string $value);

    /**
     * Makes one attempt to acquire lock with $key
     * @param string $key
     * @return bool is locking successful
     */
    public function tryLock(string $key): bool;

    /**
     * Increases value stored at $key by $amount
     * @param string $key
     * @param int $amount
     */
    public function incrKey(string $key, int $amount = 1);

    /**
     * Returns string value stored at $key
     * @param string $key
     * @return string
     */
    public function getByKey(string $key): string;

    /**
     * Checks if $key exists in the data base
     * @param string $key
     * @return bool
     */
    public function keyExists(string $key): bool;

    /**
     * Delete values with keys from $keys
     * @param array $keys
     */
    public function deleteKeys(array $keys);

    /**
     * Delete value with $key
     * @param string $key
     * @return mixed
     */
    public function delete(string $key);

    /**
     * Returns length of list stored at $key
     * @param string $key
     * @return int
     */
    public function lenByKey(string $key): int;

    /**
     * Returns element of list stored at $key by its index
     * @param string $key
     * @param int $index
     * @return string
     */
    public function getByIndex(string $key, int $index): string;

    /**
     * Stores value by $key and nested $hashKey
     * @param string $key
     * @param string $hashKey
     * @param string $value
     */
    public function setByHash(string $key, string $hashKey, string $value);

    /**
     * Appends $value to list stored at $key
     * @param string $key
     * @param string $value
     */
    public function pushRightByKey(string $key, string $value);
}
