<?php

namespace StreamCounterTask;

use Redis;

/**
 * Based on https://github.com/ronnylt/redlock-php/blob/master/src/RedLock.php
 */
class RedLock
{
    private $retryDelay;
    private $timeout;
    private $clockDriftFactor = 0.01;

    private $quorum;

    private $server;
    private $instances = array();

    /**
     * @var array info about current locking
     */
    private $lockingInfo;

    function __construct(Redis $server, $retryDelay = 100, $timeout = 5 * 60)
    {
        $this->server = $server;

        $this->retryDelay = $retryDelay;
        $this->timeout = $timeout;

        $this->quorum = 1;
    }

    public function lock($resource, $ttl)
    {
        $this->initInstances();

        $token = uniqid();
        $startTime = time();

        do {
            $n = 0;

            $curTime = microtime(true) * 1000;

            foreach ($this->instances as $instance) {
                if ($this->lockInstance($instance, $resource, $token, $ttl)) {
                    $n++;
                }
            }

            # Add 2 milliseconds to the drift to account for Redis expires
            # precision, which is 1 millisecond, plus 1 millisecond min drift
            # for small TTLs.
            $drift = ($ttl * $this->clockDriftFactor) + 2;

            $validityTime = $ttl - (microtime(true) * 1000 - $curTime) - $drift;

            if ($n >= $this->quorum && $validityTime > 0) {
                $this->lockingInfo['validity'] = $validityTime;
                $this->lockingInfo['resource'] = $resource;
                $this->lockingInfo['token'] = $token;
                return true;
            } else {
                foreach ($this->instances as $instance) {
                    $this->unlockInstance($instance, $resource, $token);
                }
            }

            // Wait a random delay before to retry
            $delay = mt_rand(floor($this->retryDelay / 2), $this->retryDelay);
            usleep($delay * 1000);

        } while (time() - $startTime > $this->timeout);

        return false;
    }

    public function unlock()
    {
        $this->initInstances();
        $resource = $this->lockingInfo['resource'];
        $token    = $this->lockingInfo['token'];

        foreach ($this->instances as $instance) {
            $this->unlockInstance($instance, $resource, $token);
        }
    }

    private function initInstances()
    {
        if (empty($this->instances)) {
            $this->instances[] = $this->server;
        }
    }

    private function lockInstance(Redis $instance, $resource, $token, $ttl)
    {
        return $instance->set($resource, $token, ['NX', 'PX' => $ttl]);
    }

    private function unlockInstance($instance, $resource, $token)
    {
        $script = '
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        ';
        return $instance->eval($script, [$resource, $token], 1);
    }
}