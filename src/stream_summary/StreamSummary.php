<?php

namespace StreamCounterTask;

include "DoublyLinkedList.php";
include "Bucket.php";
include "Counter.php";

class StreamSummary
{
    /** @var int */
    private $capacity;

    /** @var int */
    private $bucket_cnt = 0;

    /** @var array */
    private $counter_map = array();

    /** @var DoublyLinkedList */
    private $bucket_list;

    public function __construct(int $capacity)
    {
        $this->capacity = $capacity;
        $this->bucket_list = new DoublyLinkedList();
    }

    public function size(): int
    {
        return count($this->counter_map);
    }

    public function offer($item, int $inc_count = 1): bool
    {
        $is_new_item = !array_key_exists($item, $this->counter_map);
        $dropped_item = null;
        if ($is_new_item) {
            if ($this->size() < $this->capacity) {
                $counter_id = $this->size();
                $bucket = $this->bucket_list->appendRight(new Bucket($this->bucket_cnt, 0))->value;
                $counter_node = $bucket->counters->appendRight(new Counter($counter_id, $this->bucket_list->last(), $item));
                $this->bucket_cnt += 1;
            } else {
                $min_bucket = $this->bucket_list->last()->value;
                $counter_node = $min_bucket->counters->last();
                $counter = $counter_node->value;
                $dropped_item = $counter->item;
                unset($this->counter_map[$dropped_item]);
                $counter->item = $item;
                $counter->error = $min_bucket->count;
            }
            $this->counter_map[$item] = $counter_node;
        } else {
            $counter_node = $this->counter_map[$item];
        }
        $this->increment_counter($counter_node, $inc_count);
        return $is_new_item;
    }


    private function increment_counter(DLLNode $counter_node, int $inc_count)
    {
        $counter = $counter_node->value;
        $oldNode = $counter->bucket;
        $bucket = $oldNode->value;
        $bucket->counters->remove($counter_node);
        $counter->count = $counter->count + $inc_count;
        $item = $counter->item;

        $bucketNodePrev = $oldNode;
        $bucketNodeNext = $bucketNodePrev->prev;
        while ($bucketNodeNext != null) {
            $bucketNext = $bucketNodeNext->value;
            if ($counter->count == $bucketNext->count) {
                $counter_node = $bucketNext->counters->appendRight($counter_node->value);
                $this->counter_map[$item] = $counter_node;
                break;
            } else if ($counter->count > $bucketNext->count) {
                $bucketNodePrev = $bucketNodeNext;
                $bucketNodeNext = $bucketNodePrev->prev;
            } else {
                $bucketNodeNext = null;
            }
        }

        if ($bucketNodeNext == null) {
            $bucketNext = new Bucket($this->bucket_cnt, $counter->count);
            $this->bucket_cnt += 1;
            $counter_node = $bucketNext->counters->insert($counter_node->value);
            $this->counter_map[$item] = $counter_node;
            $bucketNodeNext = $this->bucket_list->insert($bucketNext, $bucketNodePrev);
        }
        $counter->bucket = $bucketNodeNext;
        if (count($bucket->counters) == 0) {
            $this->bucket_list->remove($oldNode);
        }
    }

    public function top_k(int $k): array {
        $topK = array();
        $index = count($this->bucket_list);
        while ($index) {
            $b = $this->bucket_list[--$index];
            foreach ($b->counters as $c) {
                if (count($topK) == $k) {
                    return $topK;
                }
                $topK[$c->item] = $c->count - $c->error;
            }
        }
        return $topK;
    }

    public function merge(StreamSummary $other) {
        $common_words = array_intersect(array_keys($this->counter_map), array_keys($other->counter_map));
        foreach ($common_words as $word) {
            $counter = $other->counter_map[$word]->value;
            $this->offer($word, $counter->count - $counter->error);
        }
        foreach ($common_words as $word) {
            $bucketNode = $other->counter_map[$word]->value->bucket;
            $bucketNode->value->remove($other->counter_map[$word]);
            if (count($bucketNode->value->counters) == 0) {
                $other->bucket_list->remove($bucketNode);
            }
            unset($other->counter_map);
        }

        $it1 = $this->bucket_list->first();
        $it2 = $other->bucket_list->first();
        $current_size = 0;
        while ($current_size < $this->capacity) {
            if ($it1 == null or $it2 == null) {
                break;
            }
            if ($it1->value->count == $it2->value->count) {
                $current_size = $this->merge_buckets($it2, $current_size, $it1->value);
                $it1 = $it1->next;
                $it2 = $it2->next;
            } else if ($it1->value->count > $it2->value->count) {
                $current_size += count($it1->value->counters);
                $it1 = $it1->next;
            } else {
                $new_bucket = new Bucket($this->bucket_cnt, $it2->value->count);
                $this->bucket_cnt++;
                if ($it1 != null) {
                    $bucketNode = $this->bucket_list->insert($new_bucket, $it1);
                } else {
                    $bucketNode = $this->bucket_list->appendRight($new_bucket);
                }
                $current_size = $this->merge_buckets($it2, $current_size, $bucketNode);
                $it2 = $it2->next;
            }
        }
        while ($it2 != null and $current_size < $this->capacity) {
            $new_bucket = new Bucket($this->bucket_cnt, $it2->value->count);
            $this->bucket_cnt++;
            if ($it1 != null) {
                $bucketNode = $this->bucket_list->insert($new_bucket, $it1);
                printf("insert bucket\n");
            } else {
                $bucketNode = $this->bucket_list->appendRight($new_bucket);
                printf("add bucket to end\n");
            }
            $current_size = $this->merge_buckets($it2, $current_size, $bucketNode);
            echo "current size: $current_size\n";
            $it2 = $it2->next;
        }
        # TODO: for best results all counters in bucket should be sorted in ascending order of the error rate
        $bucket_it = $this->bucket_list->last();
        while ($bucket_it != null and $current_size >= $this->capacity) {
            $counter_it = $bucket_it->value->counters->last();
            while ($counter_it != null and $current_size >= $this->capacity) {
                unset($this->counter_map[$counter_it->value->item]);
                $bucket_it->value->counters->remove($counter_it);
                $counter_it = $counter_it->prev;
                $current_size--;
            }
        }
    }

    private function merge_buckets(DLLNode $new_bucket_node, int $current_size, DLLNode $bucketNode) {
        $counter_it = $new_bucket_node->value->counters->last();
        while ($counter_it != null and $current_size < $this->capacity) {
            $counter_it->value->id = $this->size();
            echo "add counter:".strval($counter_it->value->item)."\n";
            $new_counter = clone $counter_it->value;
            $new_counter->bucket = $bucketNode;
            $new_counter->id = $current_size;
            $current_size += 1;
            $counter_node = $bucketNode->value->counters->appendRight($new_counter);
            $this->counter_map[$new_counter->item] = $counter_node;
            $counter_it = $counter_it->prev;
        }
        return $current_size;
    }

    public function save(DBTopKManager $dbManager, string $key) {
        try {
            if ($dbManager->lock($key)) {
                $dbManager->storeByKey($key."Cap", $this->capacity);
                $dbManager->storeByKey($key."BucketCount", $this->bucket_cnt);

                $dbManager->delete($key."CountersMap");
                $dbManager->setMap($key."CountersMap", array_map(function($counterNode) {
                    return $counterNode->value->id;
                }, $this->counter_map));
                # store all Buckets
                $dbManager->delete($key."Buckets");
                foreach ($this->bucket_list as $bucket) {
                    echo "BUCKETS_I: ".strval($bucket)."\n";
                    $dbManager->pushRightByKey($key."Buckets", strval($bucket));
                }
                # store all Counters
                $dbManager->delete($key."Counters");
                foreach (array_values($this->counter_map) as $counterNode) {
                    $dbManager->setByHash($key."Counters", $counterNode->value->id, Counter::store($counterNode));
                }
            }
        } finally {
            $dbManager->unlock($key);
        }
    }

    public static function load(DBTopKManager $dbManager, string $key, int $defaultCapacity): StreamSummary {
        if (!$dbManager->keyExists($key."Cap")) {
            return new StreamSummary($defaultCapacity);
        }
        $capacity = intval($dbManager->getByKey($key."Cap"));
        $bucket_cnt = intval($dbManager->getByKey($key."BucketCount"));
        $counters_raw = $dbManager->getMap($key."Counters", "strval");
        $counter_map = $dbManager->getMap($key."CountersMap", "strval");
        $counters = array();
        foreach ($counters_raw as $keyI => $counter) {
            $counters[intval($keyI)] = Counter::parse($counter);
        }

        foreach (array_values($counters) as $counter) {
            $counter->next = StreamSummary::counter_from_id($counters, $counter->next);
            $counter->prev = StreamSummary::counter_from_id($counters, $counter->prev);
        }

        $bucket_map = array();
        $bucket_list = new DoublyLinkedList();
        $prev_bucket = null;
        for ($i = 0; $i < $dbManager->lenByKey($key."Buckets"); ++$i) {
            $b = $dbManager->getByIndex($key."Buckets", $i);
            $bucket = Bucket::parse($b, $counters);
            $prev_bucket = $bucket_list->insert($bucket, $prev_bucket);
            $bucket_map[$bucket->id] = $prev_bucket;
        }

        $result = new StreamSummary($capacity);
        $result->bucket_cnt = $bucket_cnt;
        $result->bucket_list = $bucket_list;

        $result->counter_map = array();
        foreach ($counter_map as $keyI => $counter) {
            $result->counter_map[$keyI] = StreamSummary::counter_from_id($counters, $counter);
        }
        foreach (array_values($counters) as $counter) {
            $counter->value->bucket = $bucket_map[$counter->value->bucket];
        }
        return $result;
    }

    private static function counter_from_id(array $counters, int $id) {
        if ($id == -1) {
            return null;
        }
        return $counters[intval($id)];
    }

    public function getKOrderStat(int $k) {
        $bucket = $this->bucket_list->first();
        $result = 0;
        while ($k > 0 && $bucket != null) {
            $result = $bucket->value->count;
            $k -= count($bucket->value->counters);
            $bucket = $bucket->next;
        }
        return $result;
    }

    public function filtered(int $k) {
        $result = new StreamSummary($this->capacity);
        $bucket = $this->bucket_list->first();
        $lastBucket = null;
        while ($bucket != null and $bucket->value->count >= $k) {
            $result->bucket_cnt += 1;
            $lastBucket = $result->bucket_list->insert($bucket->value, $lastBucket);
            $count_node = $bucket->value->counters->last();
            while ($count_node != null) {
                $result->counter_map[$count_node->value->item] = $count_node;
                $count_node = $count_node->prev;
            }
            $bucket = $bucket->next;
        }
        return $result;
    }

    public function keys() {
        return array_keys($this->counter_map);
    }

    public function getFreq($item): int {
        if (!$this->keyExists($item)) {
            return 0;
        }
        $counter = $this->counter_map[$item]->value;
        return $counter->count;
    }

    public function itemsFreqs(): array {
        return array_map(function ($node) { return $node->value->count; }, $this->counter_map);
    }

    public function keyExists($item): bool {
        return array_key_exists($item, $this->counter_map);
    }
}