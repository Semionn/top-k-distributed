<?php

namespace StreamCounterTask;

interface TopKSolver
{
    public function processText(string $text);
    public function processWord(string $word);

    /**
     * @param string $key
     * @return bool|array
     */
    public function getTopK(string $key);
    public function setDBTopKManager(DBTopKManager $dbManager);
}
