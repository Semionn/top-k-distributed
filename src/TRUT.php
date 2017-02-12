<?php

namespace StreamCounterTask;

include 'utils.php';


class TRUT implements TopKSolver
{
    const ROUND_1_WAIT_TIME = 3; // seconds
    const ROUND_2_WAIT_TIME = 3;
    const ROUND_3_WAIT_TIME = 3;
    private $words;
    private $dbManager;
    private $k;
    private $timeFrame;
    private $lastTimestamp = 0;

    function __construct(DBTopKManager $dbManager, int $k, int $timeFrame) {
        $this->words = array();
        $this->dbManager = $dbManager;
        $this->k = $k;
        $this->timeFrame = $timeFrame;
    }

    public function setDBTopKManager(DBTopKManager $dbManager) {
        $this->dbManager = $dbManager;
    }

    public function getTopK(string $key) {
        $this->checkTimestamp();
        if (!$this->dbManager->keyExists($key."Result")) {
            return false;
        }
        return $this->dbManager->getMap($key."Result", 'intval');
    }

    public function processText(string $text) {
        $this->checkTimestamp();
        foreach (explode(" ", $text) as $word) {
            if ($this->checkWord($word)) {
                $this->processWord($word);
            }
        }
    }

    public function processWord(string $word) {
        if (!array_key_exists($word, $this->words)) {
            $this->words[$word] = 1;
        } else {
            $this->words[$word] += 1;
        }
    }

    /**
     * Can be uncomment for single-node application server
     */
    private function checkTimestamp() {
//        if (time() - $this->lastTimestamp >= $this->timeFrame) {
//            if ($this->lastTimestamp == 0) {
//                $this->lastTimestamp = (intval(time() / ($this->timeFrame)) - 1) * $this->timeFrame;
//            }
//            $key = strval($this->lastTimestamp);
//            $this->startSync($key);
//            $this->lastTimestamp = (intval(time() / ($this->timeFrame))) * $this->timeFrame;
//            $this->words = array();
//        }
    }

    public function startSync(string $key) {
        $this->phaseFirst($key);
        $this->phaseSecond($key);
        $this->phaseThird($key);
    }

    private function checkWord(string $word): bool {
        // TODO:
        return true;
    }

    /**
     * Sends top-k words with their frequencies
     * @param string $key timestamp to process
     */
    private function phaseFirst(string $key) {
        $locked = false;
        try {
            if ($this->dbManager->lock($key)) {
                $locked = True;
                $dbWords = $this->dbManager->getMap($key."Dict1", 'intval');
                echo "dbWords:";
                print_r($dbWords);
                echo "this->words:";
                print_r($this->words);
                $mergedWords = mergeDicts($dbWords, $this->words);
                echo "mergedWords:";
                print_r($mergedWords);
                $this->dbManager->setMap($key."Dict1", $mergedWords);
                $this->dbManager->incrKey($key."NodesCount");
            } else {
                error_log("Something gone wrong with locking key ".$key);
            }
        } finally {
            if ($locked)
                $this->dbManager->unlock($key);
        }
        checkAndLog(sleep(TRUT::ROUND_1_WAIT_TIME), "Phase 1 sleep failed");
        if ($this->dbManager->tryLock($key)) {
            echo "trylock1<br>";
            try {
                if ($this->dbManager->keyExists($key."t1")) {
                    $this->phaseFirstCompletion($key);
                }
            } finally {
                $this->dbManager->unlock($key);
            }
        }
    }

    private function phaseFirstCompletion($key) {
        $words = $this->dbManager->getMap($key."Dict1", 'intval');
        $t1 = getKOrderStat($words, $this->k);
        $nodes_count = intval($this->dbManager->getByKey($key."NodesCount"));
        $t1 = intval($t1 / $nodes_count);
        $this->dbManager->storeByKey($key."t1", $t1);
        echo "t1:";
        print_r($t1);
    }

    /**
     * Prune ineligible words with count less then T1
     * @param string $key timestamp to process
     */
    private function phaseSecond(string $key) {
        $locked = false;
        try {
            if ($this->dbManager->lock($key)) {
                $locked = true;
                $t1 = intval($this->dbManager->getByKey($key."t1"));

                $curWords = array_filter($this->words, function($x) use ($t1) { return $x >= $t1;});

                $dbWords = $this->dbManager->getMap($key."Dict2", 'intval');
                echo "dbWords:";
                print_r($dbWords);
                $mergedWords = mergeDicts($dbWords, $curWords);
                echo "mergedWords:";
                print_r($mergedWords);
                $this->dbManager->setMap($key."Dict2", $mergedWords);
                # increment count of nodes, which sent words from the current dictionary
                $nodesPerWord = $this->dbManager->getMap($key."NodesPerWord", 'intval');

                $commonWords = array_intersect_key($nodesPerWord, $curWords);
                echo "commonWords:";
                print_r($commonWords);
                $diffWords = array_fill_keys(array_keys(array_diff_key($curWords, $nodesPerWord)), 1);
                echo "diffWords:";
                print_r($diffWords);
                $nodesPerWord = array_merge($nodesPerWord, $diffWords);
                foreach (array_keys($commonWords) as $key) {
                    $nodesPerWord[$key] += 1;
                }
                $this->dbManager->setMap($key."NodesPerWord", $nodesPerWord);
                echo "nodesPerWord:";
                print_r($nodesPerWord);
            } else {
                error_log("Something gone wrong with locking key ".$key);
            }
        } finally {
            if ($locked)
                $this->dbManager->unlock($key);
        }
        checkAndLog(sleep(TRUT::ROUND_2_WAIT_TIME), "Phase 2 sleep failed");
        if ($this->dbManager->tryLock($key)) {
            echo "trylock2<br>";
            try {
                if (!$this->dbManager->keyExists($key."S"))
                    $this->phaseSecondCompletion($key);
            } finally {
                $this->dbManager->unlock($key);
            }
        }
    }

    private function phaseSecondCompletion(string $key)
    {
        $words = $this->dbManager->getMap($key."Dict2", 'intval');
        $t2 = getKOrderStat($this->words, $this->k);
        $nodesCount = intval($this->dbManager->getByKey($key."NodesCount"));
        $nodesPerWord = $this->dbManager->getMap($key."NodesPerWord", 'intval');
        $wordsUpperFreq = $words;
        array_walk($wordsUpperFreq, function (&$value, $word) use ($nodesCount, $nodesPerWord, $t2)
            {
                $value += ($nodesCount - $nodesPerWord[$word]) * intval($t2 / $nodesCount);
            });

        $wordsCandidatesKeys = array_keys(array_filter($wordsUpperFreq, function($x) use ($t2) { return $x >= $t2;}));
        $wordsCandidates = array_fill_keys($wordsCandidatesKeys, 0);
        echo "wordsCandidates:";
        print_r($wordsCandidates);
        $this->dbManager->setMap($key."S", $wordsCandidates);
        echo "t2:";
        print_r($t2);
    }

    /**
     * Identify top-k words from candidate set
     * @param string $key timestamp to process
     */
    private function phaseThird(string $key) {
        $locked = false;
        try {
            if ($this->dbManager->lock($key)) {
                $locked = true;
                $dbWords = $this->dbManager->getMap($key."S", 'intval');
                array_walk($dbWords, function(&$value, $word) {
                    if (array_key_exists($word, $this->words)) {
                        $value += $this->words[$word];
                    }
                });
                $this->dbManager->setMap($key."S", $dbWords);
            } else {
                error_log("Something gone wrong with locking key ".$key);
            }
        } finally {
            if ($locked)
                $this->dbManager->unlock($key);
        }
        checkAndLog(sleep(TRUT::ROUND_3_WAIT_TIME), "Phase 3 sleep failed");
        if ($this->dbManager->tryLock($key)) {
            try {
                if (!$this->dbManager->keyExists($key . "Result"))
                    $this->phaseThirdCompletion($key);
            } finally {
                $this->dbManager->unlock($key);
            }
        }
    }

    private function phaseThirdCompletion(string $key) {
        $words = $this->dbManager->getMap($key."S", 'intval');
        print_r($words);
        arsort($words);
        print_r($words);
        $result = array_slice($words, 0, $this->k, $preserve_keys = true);
        print_r($result);
        $this->dbManager->setMap($key."Result", $result);
        echo "RESULT:";
        print_r($result);
        $keysToRemove = array("NodesCount", "NodesPerWord", "S", "Dict1", "Dict2");
        $keysToRemove = array_map(function($x) use ($key) {
            return $key.$x;
        }, $keysToRemove);
        $this->dbManager->deleteKeys($keysToRemove);
    }
}