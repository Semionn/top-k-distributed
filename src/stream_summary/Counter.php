<?php

namespace StreamCounterTask;

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

    public static function parse(string $str): DLLNode {
        echo "COUNTER:".$str.":COUNTER\n";
        $arr = explode(',', $str);
        $prev_id = intval($arr[0]);
        $next_id = intval($arr[1]);
        $id = intval($arr[2]);
        $bucket_id = intval($arr[3]);
        $item = $arr[4];
        $error = intval($arr[5]);
        $count = intval($arr[6]);
        $counter = new Counter($id, $bucket_id, $item, $error, $count);
        $node = new DLLNode($counter, $prev=$prev_id, $next=$next_id);
        return $node;
    }

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
        echo "COUNTER______:".strval($counterNode->value)."\n";
        return implode(",", array_map('strval', $res));
    }

    function __toString()
    {
        return implode(",", array_map('strval', [$this->id, $this->bucket->value->id, $this->item, $this->error, $this->count]));
    }
}
