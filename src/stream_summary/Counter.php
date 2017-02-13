<?php

namespace StreamCounterTask;

/**
 * Data structure for the <i>StreamSummary</i>.
 * Contains link to parent bucket, item for counting, base frequency end estimate of counting error
 * @package StreamCounterTask
 */
class Counter
{
    /** @var int */
    public $id;

    /** @var DLLNode|int */
    public $bucket;

    /** @var mixed */
    public $item;

    /** @var int */
    public $error;

    /** @var int */
    public $count;

    function __construct(int $id, $bucket, $item, int $error=0, int $count=0) {
        $this->id = $id;
        $this->bucket = $bucket;
        $this->item = $item;
        $this->error = $error;
        $this->count = $count;
    }

    /**
     * Parses string representation of a <i>Counter</i>
     * @param string $str
     * @return DLLNode new <i>DLLNode</i>, which contains counter with parsed fields and links (as IDs) to other nodes
     */
    public static function parse(string $str): DLLNode {
        $arr = explode(',', $str);
        $prevID = intval($arr[0]);
        $nextID = intval($arr[1]);
        $id = intval($arr[2]);
        $bucketID = intval($arr[3]);
        $item = $arr[4];
        $error = intval($arr[5]);
        $count = intval($arr[6]);
        $counter = new Counter($id, $bucketID, $item, $error, $count);
        $node = new DLLNode($counter, $prev=$prevID, $next=$nextID);
        return $node;
    }

    /**
     * Returns string representation of parent <i>DLLNode</i> and itself
     * @param DLLNode $counterNode parent node
     * @return string
     */
    public static function store(DLLNode $counterNode): string
    {
        $res = array();
        if ($counterNode->prev != null) {
            $res[] = $counterNode->prev->value->id;
        } else {
            $res[] = -1;
        }
        if ($counterNode->next != null) {
            $res[] = $counterNode->next->value->id;
        } else {
            $res[] = -1;
        }
        $res[] = strval($counterNode->value);
        return implode(",", array_map('strval', $res));
    }

    /**
     * String representation of <i>Counter</i> for storing in the data base
     * @return string
     */
    function __toString()
    {
        return implode(",", array_map('strval', [$this->id, $this->bucket->value->id, $this->item, $this->error, $this->count]));
    }
}
