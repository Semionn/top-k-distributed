<?php

namespace StreamCounterTask;

interface TopKSolver
{
    public function processText(string $text);
    public function processWord(string $word);
    public function getTopK(string $key);
    public function setDBTopKManager(DBTopKManager $db_manager);
}
