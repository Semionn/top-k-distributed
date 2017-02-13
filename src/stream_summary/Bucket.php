<?php

namespace StreamCounterTask;

class Bucket
{
    /** @var int */
    public $id;

    /** @var DoublyLinkedList */
    public $counters;

    /** @var int */
    public $count;

    public function __construct(int $id, int $count) {
        $this->id = $id;
        $this->counters = new DoublyLinkedList();
        $this->count = $count;
    }

    public function remove(DLLNode $node) {
        $this->counters->remove($node);
    }

    public static function parse(string $str, array $counters): Bucket {
        $arr = explode(',', $str);
        $id = intval($arr[0]);
        $count = intval($arr[1]);
        $counters_size = intval($arr[2]);
        $last_id = intval($arr[3]);
        $result = new Bucket($id, $count);
        $result->counters = new DoublyLinkedList($counters[$last_id], $counters_size);
        return $result;
    }

    function __toString() {
        return implode(",", array_map('strval', [$this->id, $this->count, count($this->counters), $this->counters->last()->value->id]));
    }
}