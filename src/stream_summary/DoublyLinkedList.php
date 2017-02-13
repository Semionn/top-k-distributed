<?php
namespace StreamCounterTask;

use Countable;
use Iterator;
use IteratorAggregate;

/**
 * Doubly Linked List
 * The only reason to use instead of <i>SplDoublyLinkedList</i> is <i>DLLNode</i>s referencing
 * @package StreamCounterTask
 */
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
     * Returns the first node of the list at linear time
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
     * Returns the last node of the list
     * @return null|DLLNode
     */
    public function last() {
        return $this->last;
    }

    /**
     * Appends the item at the end of the list
     * @param $item
     * @return DLLNode node with inserted item
     */
    public function appendRight($item) {
        $newNode = new DLLNode($item);
        $this->size += 1;
        if ($this->last == null) {
            $this->last = $newNode;
            return $newNode;
        }
        $this->last->next = $newNode;
        $newNode->prev = $this->last;
        $this->last = $newNode;
        return $newNode;
    }

    /**
     * Inserts the item before specified node, or at the end, if the node is null
     * @param $item
     * @param null $beforeNode
     * @return DLLNode node with inserted item
     */
    public function insert($item, $beforeNode = null) {
        if ($beforeNode == null) {
            return $this->appendRight($item);
        }
        $new_node = new DLLNode($item);
        $this->size += 1;
        $new_node->prev = $beforeNode->prev;
        $new_node->next = $beforeNode;
        if ($beforeNode->prev != null) {
            $beforeNode->prev->next = $new_node;
        }
        $beforeNode->prev = $new_node;
        return $new_node;
    }

    /**
     * Removes specified node from the list
     * @param DLLNode $node
     */
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

/**
 * Node for the <i>DoublyLinkedList</i>
 * @package StreamCounterTask
 */
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

/**
 * Iterator implementation for the <i>DoublyLinkedList</i>
 * @link http://php.net/manual/en/class.iteratoraggregate.php
 * @package StreamCounterTask
 */
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
