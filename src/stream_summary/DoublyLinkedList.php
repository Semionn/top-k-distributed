<?php
namespace StreamCounterTask;

use Countable;
use Iterator;
use IteratorAggregate;

class DoublyLinkedList implements Countable, IteratorAggregate
{
    /** @var null|DLLNode */
    private $last;

    /** @var int */
    private $size;

    public function __construct($last=null, int $size=0)
    {
        $this->last = $last;
        $this->size = $size;
    }

    /**
     * @return null|DLLNode
     */
    public function first() {
        $res = $this->last;
        if ($res != null) {
            while ($res->prev != null) {
                $res = $res->prev;
            }
        }
        return $res;
    }

    /**
     * @return null|DLLNode
     */
    public function last() {
        return $this->last;
    }


    public function appendRight($item) {
        $new_node = new DLLNode($item);
        $this->size += 1;
        if ($this->last == null) {
            $this->last = $new_node;
            return $new_node;
        }
        $this->last->next = $new_node;
        $new_node->prev = $this->last;
        $this->last = $new_node;
        return $new_node;
    }

    public function insert($item, $before_node = null) {
        if ($before_node == null) {
            return $this->appendRight($item);
        }
        $new_node = new DLLNode($item);
        $this->size += 1;
        $new_node->prev = $before_node->prev;
        $new_node->next = $before_node;
        if ($before_node->prev != null) {
            $before_node->prev->next = $new_node;
        }
        $before_node->prev = $new_node;
        return $new_node;
    }

    public function remove(DLLNode $node) {
        $this->size -= 1;
        if ($node == $this->last) {
            $this->last = $node->prev;
        }
        if ($node->prev != null) {
            $node->prev->next = $node->next;
        }
        if ($node->next != null) {
            $node->next->prev = $node->prev;
        }
    }

    /**
     * @link http://php.net/manual/en/countable.count.php
     * @return int count of nodes in list
     */
    public function count(): int {
        return $this->size;
    }

    /**
     * @link http://php.net/manual/en/class.iteratoraggregate.php
     * @return Iterator
     */
    public function getIterator(): Iterator
    {
        return new DLLIterator($this);
    }
}

class DLLNode
{
    /** @var mixed */
    public $value;

    /** @var null|DLLNode */
    public $prev;

    /** @var null|DLLNode */
    public $next;

    public function __construct($value, $prev=null, $next=null) {
        $this->value = $value;
        $this->prev = $prev;
        $this->next = $next;
    }
}

class DLLIterator implements Iterator
{
    /** @var DoublyLinkedList */
    private $linkedList;

    /** @var null|DLLNode */
    private $node;

    public function __construct(DoublyLinkedList $linkedList) {
        $this->linkedList = $linkedList;
        $this->node = $linkedList->last();
    }

    public function rewind() {
        $this->node = $this->linkedList->last();
    }

    public function current() {
        return $this->node->value;
    }

    public function key() {
        return $this->node;
    }

    public function next() {
        $this->node = $this->node->prev;
        return $this->node;
    }

    public function valid(): bool {
        return $this->node != null;
    }

}
