<?php

namespace StreamCounterTask;

/**
 * Interface for Top-K frequent items task solvers
 * @package StreamCounterTask
 */
interface TopKSolver
{
    /**
     * Process $text - splits it onto "words" and process each word separately
     * @param string $text
     */
    public function processText(string $text);

    /**
     * Process string as "word" - increases stored frequency of the word by 1
     * @param string $word
     */
    public function processWord(string $word);

    /**
     * Returns false, if no results found at $key, and array of items with estimated frequencies otherwise
     * @param string $key
     * @return bool|array
     */
    public function getTopK(string $key);

    /**
     * Sets $dbManager to use for the rest operations
     * @param DBTopKManager $dbManager
     */
    public function setDBTopKManager(DBTopKManager $dbManager);
}
