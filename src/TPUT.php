<?php

namespace StreamCounterTask;

include 'utils.php';
include 'stream_summary/StreamSummary.php';

/**
 * Implementation of <i>TopKSolver</i> based on paper:
 * <i>Efficient Top-K Query Calculation in Distributed Networks</i>
 * by Pei Cao and Zhe Wang, 2004 <p>
 * Distributed and exact version of solving top-k problem.
 * Improves total complexity of solving the task by N distributed nodes
 * from O(N^2^) to O(N^1.5^) in case of Zipf distribution <p>
 * There are more fast improvement of this algorithm, named 4RUT
 * (<i>Efficient Top-k Query Processing Algorithms in Highly Distributed Environments</i>
 * by Qiming Fang, Guangwen Yang, 2014), but in the current project implementation,
 * without allotted servers to aggregation, 4RUT would be slower
 * @package StreamCounterTask
 */
class TPUT implements TopKSolver
{
    const ROUND_1_WAIT_TIME = 3; // seconds
    const ROUND_2_WAIT_TIME = 3;
    const ROUND_3_WAIT_TIME = 3;
    const PHASE_1_KEY_DICT  = "Dict1";
    const PHASE_1_KEY_NODES = "NodesCount";
    const PHASE_2_KEY_DICT  = "Dict2";
    const PHASE_2_KEY_NODES = "NodesPerWord";
    const PHASE_2_KEY_SURV  = "S";
    const FINAL_KEY_RESULT  = "Result";
    const PHASE_1_KEY_T = "t1";

    /** @var StreamSummary */
    private $words;

    /** @var DBTopKManager */
    private $dbManager;

    /** @var int */
    private $k;

    /** @var int */
    private $timeFrame;

    /** @var int */
    private $dbCapacity;

    /** @var int */
    private $lastTimestamp = 0;

    function __construct(DBTopKManager $dbManager, int $k, int $timeFrame, int $capacity, int $dbCapacity) {
        $this->words = new StreamSummary($capacity);
        $this->dbManager = $dbManager;
        $this->k = $k;
        $this->timeFrame = $timeFrame;
        $this->dbCapacity = $dbCapacity;
    }

    public function setDBTopKManager(DBTopKManager $dbManager) {
        $this->dbManager = $dbManager;
    }

    public function getTopK(string $key) {
        $this->checkTimestamp();
        if (!$this->dbManager->keyExists($key. TPUT::FINAL_KEY_RESULT)) {
            return false;
        }
        return $this->dbManager->getMap($key. TPUT::FINAL_KEY_RESULT, 'intval');
    }

    /**
     * Process a lowered words from the text
     * @param string $text
     */
    public function processText(string $text) {
        $this->checkTimestamp();
        $delim = " \n\t,.!?:;()";
        $word = strtok(strtolower($text), $delim);
        while ($word !== false) {
            if ($this->checkWord($word)) {
                $this->processWord($word);
            }
            $word = strtok($delim);
        }
    }

    /**
     * Process $word without checking
     * @param string $word
     */
    public function processWord(string $word) {
        $this->words->offer($word);
    }

    /**
     * Starting TPUT algorithm to evaluate top-K and store it in the data base
     * @param string $key
     */
    public function startSync(string $key) {
        $this->phaseFirst($key);
        $this->phaseSecond($key);
        $this->phaseThird($key);
    }

    /**
     * Checks $word for containing at least one alphabet char
     * @param string $word
     * @return bool
     */
    private function checkWord(string $word): bool {
        return preg_match("/^.*[a-z].*$/i", $word);
    }

    /**
     * Sends top-k words with their frequencies
     * @param string $key timestamp to process
     */
    private function phaseFirst(string $key) {
        try {
            if ($this->dbManager->lock($key)) {
                $dbWords = StreamSummary::load($this->dbManager, $key. TPUT::PHASE_1_KEY_DICT, $this->dbCapacity);
                $dbWords->update($this->words);
                $dbWords->save($this->dbManager, $key. TPUT::PHASE_1_KEY_DICT);
                $this->dbManager->incrKey($key. TPUT::PHASE_1_KEY_NODES);
            } else {
                error_log("Something gone wrong with locking key ".$key);
            }
        } finally {
            $this->dbManager->unlock($key);
        }
        checkAndLog(sleep(TPUT::ROUND_1_WAIT_TIME), "Phase 1 sleep failed");
        if ($this->dbManager->tryLock($key)) {
            try {
                if (!$this->dbManager->keyExists($key. TPUT::PHASE_1_KEY_T)) {
                    $this->phaseFirstCompletion($key);
                }
            } finally {
                $this->dbManager->unlock($key);
            }
        }
    }

    private function phaseFirstCompletion(string $key) {
        $words = StreamSummary::load($this->dbManager, $key. TPUT::PHASE_1_KEY_DICT, $this->dbCapacity);
        $t1 = $words->getKOrderStat($this->k);
        $nodesCount = intval($this->dbManager->getByKey($key. TPUT::PHASE_1_KEY_NODES));
        $t1 = intval($t1 / $nodesCount);
        $this->dbManager->storeByKey($key. TPUT::PHASE_1_KEY_T, $t1);
    }

    /**
     * Prune ineligible words with count less then T1
     * @param string $key timestamp to process
     */
    private function phaseSecond(string $key) {
        try {
            if ($this->dbManager->lock($key)) {
                $t1 = intval($this->dbManager->getByKey($key. TPUT::PHASE_1_KEY_T));

                $curWords = $this->words->filtered($t1);

                $dbWords = StreamSummary::load($this->dbManager, $key. TPUT::PHASE_2_KEY_DICT, $this->dbCapacity);
                $dbWords->update($curWords);
                $dbWords->save($this->dbManager, $key. TPUT::PHASE_2_KEY_DICT);
                # increment count of nodes, which sent words same as from the current dictionary
                $nodesPerWord = $this->dbManager->getMap($key. TPUT::PHASE_2_KEY_NODES, 'intval');

                $commonWords = array_intersect(array_keys($nodesPerWord), $curWords->keys());
                $diffWords = array_fill_keys(array_diff($curWords->keys(), array_keys($nodesPerWord)), 1);
                $nodesPerWord = $nodesPerWord + $diffWords;
                foreach (array_keys($commonWords) as $keyI) {
                    $nodesPerWord[$keyI] += 1;
                }
                $this->dbManager->setMap($key. TPUT::PHASE_2_KEY_NODES, $nodesPerWord);
            } else {
                error_log("Something gone wrong with locking key ".$key);
            }
        } finally {
            $this->dbManager->unlock($key);
        }
        checkAndLog(sleep(TPUT::ROUND_2_WAIT_TIME), "Phase 2 sleep failed");
        if ($this->dbManager->tryLock($key)) {
            try {
                if (!$this->dbManager->keyExists($key. TPUT::PHASE_2_KEY_SURV))
                    $this->phaseSecondCompletion($key);
            } finally {
                $this->dbManager->unlock($key);
            }
        }
    }

    private function phaseSecondCompletion(string $key)
    {
        $words = StreamSummary::load($this->dbManager, $key. TPUT::PHASE_2_KEY_DICT, $this->dbCapacity);
        $t2 = $this->words->getKOrderStat($this->k);
        $nodesCount = intval($this->dbManager->getByKey($key. TPUT::PHASE_1_KEY_NODES));
        $nodesPerWord = $this->dbManager->getMap($key. TPUT::PHASE_2_KEY_NODES, 'intval');
        $wordsUpperFreq = $words->itemsFreqs();

        array_walk($wordsUpperFreq, function (&$value, $word) use ($nodesCount, $nodesPerWord, $t2)
            {
                $value += ($nodesCount - $nodesPerWord[$word]) * intval($t2 / $nodesCount);
            });

        $wordsCandidatesKeys = array_keys(array_filter($wordsUpperFreq, function($x) use ($t2) { return $x >= $t2;}));
        $wordsCandidates = array_fill_keys($wordsCandidatesKeys, 0);
        $this->dbManager->setMap($key. TPUT::PHASE_2_KEY_SURV, $wordsCandidates);
    }

    /**
     * Identify top-k words from candidate set
     * @param string $key timestamp to process
     */
    private function phaseThird(string $key) {
        try {
            if ($this->dbManager->lock($key)) {
                $dbWords = $this->dbManager->getMap($key. TPUT::PHASE_2_KEY_SURV, 'intval');
                array_walk($dbWords, function(&$value, $word) {
                    if ($this->words->keyExists($word)) {
                        $value += $this->words->getFreq($word);
                    }
                });
                $this->dbManager->setMap($key. TPUT::PHASE_2_KEY_SURV, $dbWords);
            } else {
                error_log("Something gone wrong with locking key ".$key);
            }
        } finally {
            $this->dbManager->unlock($key);
        }
        checkAndLog(sleep(TPUT::ROUND_3_WAIT_TIME), "Phase 3 sleep failed");
        if ($this->dbManager->tryLock($key)) {
            try {
                if (!$this->dbManager->keyExists($key. TPUT::FINAL_KEY_RESULT))
                    $this->phaseThirdCompletion($key);
            } finally {
                $this->dbManager->unlock($key);
            }
        }
    }

    private function phaseThirdCompletion(string $key) {
        $words = $this->dbManager->getMap($key. TPUT::PHASE_2_KEY_SURV, 'intval');
        arsort($words);
        $result = array_slice($words, 0, $this->k, $preserve_keys = true);
        $this->dbManager->setMap($key. TPUT::FINAL_KEY_RESULT, $result);
        echo "RESULT:";
        print_r($result);
        $keysToRemove = array(TPUT::PHASE_1_KEY_NODES, TPUT::PHASE_2_KEY_NODES, TPUT::PHASE_2_KEY_SURV, TPUT::PHASE_1_KEY_DICT, TPUT::PHASE_2_KEY_DICT, TPUT::PHASE_1_KEY_T);
        $keysToRemove = array_map(function($x) use ($key) {
            return $key.$x;
        }, $keysToRemove);
        $this->dbManager->deleteKeys($keysToRemove);
        StreamSummary::clearKeys($this->dbManager, $key. TPUT::PHASE_1_KEY_DICT);
        StreamSummary::clearKeys($this->dbManager, $key. TPUT::PHASE_2_KEY_DICT);
    }

    /**
     * Can be uncomment for single-node application
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
}