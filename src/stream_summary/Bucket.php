<?php

namespace StreamCounterTask;

/**
 * Data structure for the <i>StreamSummary</i>.
 * Contains <i>DoublyLinkedList</i> of counters and estimate of common frequency for the all stored counters.
 * @package StreamCounterTask
 */
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

    /**
     * Removes specified node from the counters list
     * @param DLLNode $node
     */
    public function remove(DLLNode $node) {
        $this->counters->remove($node);
    }

    /**
     * Parses string representation of a <i>Bucket</i>
     * @param string $str
     * @param array $counters array of counters for finding counter by it's id
     * @return Bucket new <i>Bucket</i> with parsed fields
     */
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

    /**
     * String representation of <i>Bucket</i> for storing in the data base
     * @return string
     */
    function __toString() {
        return implode(",", array_map('strval', [$this->id, $this->count, count($this->counters), $this->counters->last()->value->id]));
    }
}