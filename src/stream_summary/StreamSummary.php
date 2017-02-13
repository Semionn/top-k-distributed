<?php

namespace StreamCounterTask;

include "DoublyLinkedList.php";
include "Bucket.php";
include "Counter.php";

/**
 * StreamSummary implementation based on
 * @link https://github.com/addthis/stream-lib
 * Original paper: <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 * by Metwally, Agrawal, and Abbad, 2005
 * Brief review: this data structure allows to store a counters of items, increment them at O(1)
 * and find the item with minimum counter at O(1). With SpaceSaving algorithm it guarantees to find
 * top-k items of Zipfian distributed data.
 * @package StreamCounterTask
 */
class StreamSummary
{
    /** @var int */
    private $capacity;

    /** @var int */
    private $bucketCnt = 0;

    /** @var array */
    private $counterMap = array();

    /** @var DoublyLinkedList */
    private $bucketList;

    public function __construct(int $capacity) {
        $this->capacity = $capacity;
        $this->bucketList = new DoublyLinkedList();
    }

    /**
     * Returns count of stored items
     * @return int
     */
    public function size(): int {
        return count($this->counterMap);
    }

    /**
     * Increments counter for the specified item by $inc_count
     * @param $item
     * @param int $incCount
     * @return bool
     */
    public function offer($item, int $incCount = 1): bool {
        $isNewItem = !array_key_exists($item, $this->counterMap);
        $droppedItem = null;
        if ($isNewItem) {
            if ($this->size() < $this->capacity) {
                $counterId = $this->size();
                $bucket = $this->bucketList->appendRight(new Bucket($this->bucketCnt, 0))->value;
                $counterNode = $bucket->counters->appendRight(new Counter($counterId, $this->bucketList->last(), $item));
                $this->bucketCnt += 1;
            } else {
                $minBucket = $this->bucketList->last()->value;
                $counterNode = $minBucket->counters->last();
                $counter = $counterNode->value;
                $droppedItem = $counter->item;
                unset($this->counterMap[$droppedItem]);
                $counter->item = $item;
                $counter->error = $minBucket->count;
            }
            $this->counterMap[$item] = $counterNode;
        } else {
            $counterNode = $this->counterMap[$item];
        }
        $this->incrementCounter($counterNode, $incCount);
        return $isNewItem;
    }

    private function incrementCounter(DLLNode $counterNode, int $incCount) {
        $counter = $counterNode->value;
        $oldNode = $counter->bucket;
        $bucket = $oldNode->value;
        $bucket->counters->remove($counterNode);
        $counter->count = $counter->count + $incCount;
        $item = $counter->item;

        $bucketNodePrev = $oldNode;
        $bucketNodeNext = $bucketNodePrev->prev;
        while ($bucketNodeNext != null) {
            $bucketNext = $bucketNodeNext->value;
            if ($counter->count == $bucketNext->count) {
                $counterNode = $bucketNext->counters->appendRight($counterNode->value);
                $this->counterMap[$item] = $counterNode;
                break;
            } else if ($counter->count > $bucketNext->count) {
                $bucketNodePrev = $bucketNodeNext;
                $bucketNodeNext = $bucketNodePrev->prev;
            } else {
                $bucketNodeNext = null;
            }
        }

        if ($bucketNodeNext == null) {
            $bucketNext = new Bucket($this->bucketCnt, $counter->count);
            $this->bucketCnt += 1;
            $counterNode = $bucketNext->counters->insert($counterNode->value);
            $this->counterMap[$item] = $counterNode;
            $bucketNodeNext = $this->bucketList->insert($bucketNext, $bucketNodePrev);
        }
        $counter->bucket = $bucketNodeNext;
        if (count($bucket->counters) == 0) {
            $this->bucketList->remove($oldNode);
        }
    }

    /**
     * Returns array of k most frequent items
     * @param int $k
     * @return array
     */
    public function top_k(int $k): array {
        $topK = array();
        $index = count($this->bucketList);
        while ($index) {
            $b = $this->bucketList[--$index];
            foreach ($b->counters as $c) {
                if (count($topK) == $k) {
                    return $topK;
                }
                $topK[$c->item] = $c->count - $c->error;
            }
        }
        return $topK;
    }

    /**
     * Updates current structure with elements from the other in descending order
     * After update total count of elements won't exceeds the capacity
     * @param StreamSummary $other
     */
    public function update(StreamSummary $other) {
        $commonWords = array_intersect(array_keys($this->counterMap), array_keys($other->counterMap));
        foreach ($commonWords as $word) {
            $counter = $other->counterMap[$word]->value;
            $this->offer($word, $counter->count - $counter->error);
        }
        foreach ($commonWords as $word) {
            $bucketNode = $other->counterMap[$word]->value->bucket;
            $bucketNode->value->remove($other->counterMap[$word]);
            if (count($bucketNode->value->counters) == 0) {
                $other->bucketList->remove($bucketNode);
            }
            unset($other->counterMap);
        }

        $it1 = $this->bucketList->first();
        $it2 = $other->bucketList->first();
        $currentSize = 0;
        while ($currentSize < $this->capacity) {
            if ($it1 == null or $it2 == null) {
                break;
            }
            if ($it1->value->count == $it2->value->count) {
                $currentSize = $this->updateBucket($it2, $currentSize, $it1->value);
                $it1 = $it1->next;
                $it2 = $it2->next;
            } else if ($it1->value->count > $it2->value->count) {
                $currentSize += count($it1->value->counters);
                $it1 = $it1->next;
            } else {
                list($currentSize, $it2) = $this->insertBucket($it2, $it1, $currentSize);
            }
        }
        while ($it2 != null and $currentSize < $this->capacity) {
            list($currentSize, $it2) = $this->insertBucket($it2, $it1, $currentSize);
        }
        $bucketIt = $this->bucketList->last();
        while ($bucketIt != null and $currentSize >= $this->capacity) {
            $counterIt = $bucketIt->value->counters->last();
            while ($counterIt != null and $currentSize >= $this->capacity) {
                unset($this->counterMap[$counterIt->value->item]);
                $bucketIt->value->counters->remove($counterIt);
                $counterIt = $counterIt->prev;
                $currentSize--;
            }
        }
    }

    /**
     * Inserts new bucket from $it2 before $it1 or at the and of the bucket list
     * @param null|DLLNode $it2
     * @param null|DLLNode $it1
     * @param int $currentSize
     * @return array
     */
    private function insertBucket($it2, $it1, $currentSize): array
    {
        $newBucket = new Bucket($this->bucketCnt, $it2->value->count);
        $this->bucketCnt++;
        if ($it1 != null) {
            $bucketNode = $this->bucketList->insert($newBucket, $it1);
        } else {
            $bucketNode = $this->bucketList->appendRight($newBucket);
        }
        $currentSize = $this->updateBucket($it2, $currentSize, $bucketNode);
        $it2 = $it2->next;
        return array($currentSize, $it2);
    }

    /**
     * Updates $bucketNode with counters from the $new_bucket_node.
     * Both of them should store counters of the same frequency
     * @param DLLNode $newBucketNode
     * @param int $currentSize
     * @param DLLNode $bucketNode
     * @return int
     */
    private function updateBucket(DLLNode $newBucketNode, int $currentSize, DLLNode $bucketNode) {
        $counterIt = $newBucketNode->value->counters->last();
        while ($counterIt != null and $currentSize < $this->capacity) {
            $counterIt->value->id = $this->size();
            $newCounter = clone $counterIt->value;
            $newCounter->bucket = $bucketNode;
            $newCounter->id = $currentSize;
            $currentSize += 1;
            $counterNode = $bucketNode->value->counters->appendRight($newCounter);
            $this->counterMap[$newCounter->item] = $counterNode;
            $counterIt = $counterIt->prev;
        }
        return $currentSize;
    }

    const STREAM_SUM_KEY_CAP = "Cap";
    const STREAM_SUM_KEY_BUCK_CNT = "BucketCount";
    const STREAM_SUM_KEY_COUNTERS_MAP = "CountersMap";
    const STREAM_SUM_KEY_COUNTERS = "Counters";
    const STREAM_SUM_KEY_BUCKETS = "Buckets";

    /**
     * Deletes info of previously stored <i>StreamSummary</i> with $key prefix
     * @param DBTopKManager $dbManager
     * @param string $key
     */
    public static function clearKeys(DBTopKManager $dbManager, string $key) {
        try {
            if ($dbManager->lock($key)) {
                $dbManager->deleteKeys(array(
                    $key.StreamSummary::STREAM_SUM_KEY_CAP,
                    $key.StreamSummary::STREAM_SUM_KEY_BUCK_CNT,
                    $key.StreamSummary::STREAM_SUM_KEY_COUNTERS_MAP,
                    $key.StreamSummary::STREAM_SUM_KEY_COUNTERS,
                    $key.StreamSummary::STREAM_SUM_KEY_BUCKETS,
                ));
            }
        } finally {
            $dbManager->unlock($key);
        }
    }

    /**
     * Saves the structure with provided <i>DBTopKManager</i> and prefix $key.
     * The method acquires lock $key for the consistency of parallel savings
     * @param DBTopKManager $dbManager
     * @param string $key
     */
    public function save(DBTopKManager $dbManager, string $key) {
        try {
            if ($dbManager->lock($key)) {
                $dbManager->storeByKey($key. StreamSummary::STREAM_SUM_KEY_CAP, $this->capacity);
                $dbManager->storeByKey($key.StreamSummary::STREAM_SUM_KEY_BUCK_CNT, $this->bucketCnt);

                $dbManager->delete($key.StreamSummary::STREAM_SUM_KEY_COUNTERS_MAP);
                $dbManager->setMap($key.StreamSummary::STREAM_SUM_KEY_COUNTERS_MAP, array_map(function($counterNode) {
                    return $counterNode->value->id;
                }, $this->counterMap));
                # store all Buckets
                $dbManager->delete($key.StreamSummary::STREAM_SUM_KEY_BUCKETS);
                foreach ($this->bucketList as $bucket) {
                    $dbManager->pushRightByKey($key.StreamSummary::STREAM_SUM_KEY_BUCKETS, strval($bucket));
                }
                # store all Counters
                $dbManager->delete($key.StreamSummary::STREAM_SUM_KEY_COUNTERS);
                foreach (array_values($this->counterMap) as $counterNode) {
                    $dbManager->setByHash($key.StreamSummary::STREAM_SUM_KEY_COUNTERS, $counterNode->value->id, Counter::store($counterNode));
                }
            }
        } finally {
            $dbManager->unlock($key);
        }
    }

    /**
     * Restores <i>StreamSummary</i> from database with provided <i>DBTopKManager</i> and prefix $key
     * If the structure isn't stored, return empty instance of <i>StreamSummary</i> with $defaultCapacity
     * @param DBTopKManager $dbManager
     * @param string $key
     * @param int $defaultCapacity
     * @return StreamSummary
     */
    public static function load(DBTopKManager $dbManager, string $key, int $defaultCapacity): StreamSummary {
        if (!$dbManager->keyExists($key.StreamSummary::STREAM_SUM_KEY_CAP)) {
            return new StreamSummary($defaultCapacity);
        }
        $capacity = intval($dbManager->getByKey($key.StreamSummary::STREAM_SUM_KEY_CAP));
        $bucketCnt = intval($dbManager->getByKey($key.StreamSummary::STREAM_SUM_KEY_BUCK_CNT));
        $countersRaw = $dbManager->getMap($key.StreamSummary::STREAM_SUM_KEY_COUNTERS, "strval");
        $counterMap = $dbManager->getMap($key.StreamSummary::STREAM_SUM_KEY_COUNTERS_MAP, "strval");
        $counters = array();
        foreach ($countersRaw as $keyI => $counter) {
            $counters[intval($keyI)] = Counter::parse($counter);
        }

        // All pointers stored as IDs, and should be converted back to pointers
        foreach (array_values($counters) as $counter) {
            $counter->next = StreamSummary::counter_from_id($counters, $counter->next);
            $counter->prev = StreamSummary::counter_from_id($counters, $counter->prev);
        }

        $bucketMap = array();
        $bucketList = new DoublyLinkedList();
        $prevBucket = null;
        for ($i = 0; $i < $dbManager->lenByKey($key.StreamSummary::STREAM_SUM_KEY_BUCKETS); ++$i) {
            $b = $dbManager->getByIndex($key.StreamSummary::STREAM_SUM_KEY_BUCKETS, $i);
            $bucket = Bucket::parse($b, $counters);
            $prevBucket = $bucketList->insert($bucket, $prevBucket);
            $bucketMap[$bucket->id] = $prevBucket;
        }

        $result = new StreamSummary($capacity);
        $result->bucketCnt = $bucketCnt;
        $result->bucketList = $bucketList;

        $result->counterMap = array();
        foreach ($counterMap as $keyI => $counter) {
            $result->counterMap[$keyI] = StreamSummary::counter_from_id($counters, $counter);
        }
        foreach (array_values($counters) as $counter) {
            $counter->value->bucket = $bucketMap[$counter->value->bucket];
        }
        return $result;
    }

    private static function counter_from_id(array $counters, int $id) {
        // null pointers should be stored as -1
        if ($id == -1) {
            return null;
        }
        return $counters[intval($id)];
    }

    /**
     * Returns $k order statistics of the item frequencies
     * @param int $k
     * @return int
     */
    public function getKOrderStat(int $k): int {
        $bucket = $this->bucketList->first();
        $result = 0;
        while ($k > 0 && $bucket != null) {
            $result = $bucket->value->count;
            $k -= count($bucket->value->counters);
            $bucket = $bucket->next;
        }
        return $result;
    }

    /**
     * Returns new <i>StreamSummary</i> structure with <u>the same</u> counter nodes and buckets,
     * but only with frequency equal or greater then $k
     * @param int $k
     * @return StreamSummary
     */
    public function filtered(int $k): StreamSummary {
        $result = new StreamSummary($this->capacity);
        $bucket = $this->bucketList->first();
        $lastBucket = null;
        while ($bucket != null and $bucket->value->count >= $k) {
            $result->bucketCnt += 1;
            $lastBucket = $result->bucketList->insert($bucket->value, $lastBucket);
            $countNode = $bucket->value->counters->last();
            while ($countNode != null) {
                $result->counterMap[$countNode->value->item] = $countNode;
                $countNode = $countNode->prev;
            }
            $bucket = $bucket->next;
        }
        return $result;
    }

    /**
     * Returns array of stored items
     * @return array
     */
    public function keys() {
        return array_keys($this->counterMap);
    }

    /**
     * Returns frequency of the $item
     * @param $item
     * @return int
     */
    public function getFreq($item): int {
        if (!$this->keyExists($item)) {
            return 0;
        }
        $counter = $this->counterMap[$item]->value;
        return $counter->count;
    }

    /**
     * Returns associative array of item frequencies
     * @return array
     */
    public function itemsFreqs(): array {
        return array_map(function ($node) { return $node->value->count; }, $this->counterMap);
    }

    /**
     * Returns is the $item stored in the structure
     * @param $item
     * @return bool
     */
    public function keyExists($item): bool {
        return array_key_exists($item, $this->counterMap);
    }
}