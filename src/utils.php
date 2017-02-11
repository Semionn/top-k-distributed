<?php

function checkAndLog($functionResult, string $message) {
    if ($functionResult === false) {
        error_log($message);
    }
}

function printTimeSec(string $msg) {
    echo $msg.":".strval(time() % 60)."<br>";
}

/**
 * @param int $timeFrameSize seconds
 * @return int
 */
function getCurrentTimeFrame(int $timeFrameSize)
{
    return (intval(time() / ($timeFrameSize)) - 1) * $timeFrameSize;
}

function mergeDicts(array $dict1, array $dict2) {
    $merged_words = $dict1 + $dict2;
    $common_words = array_keys(array_intersect_key($dict1, $dict2));
    foreach ($common_words as $key) {
        $merged_words[$key] = $dict1[$key] + $dict2[$key];
    }
    return $merged_words;
}

function getKOrderStat(array $array, int $k) {
    $n = count($array);
    if ($n < $k) {
        return 0;
    }
    $arrCopy = array_values($array);
    nth_element($arrCopy, 0, $n, $n - $k - 1);
    return array_values($arrCopy)[$n - $k - 1];
}

/**
 * Randomized n_th element search with O(n) time complexity on average
 * https://sites.google.com/site/indy256/algo/kth_order_statistics
 */
function nth_element(array &$a, int $low, int $high, int $n) {
    while (true) {
        $k = randomizedPartition($a, $low, $high);
        if ($n < $k)
            $high = $k;
        else if ($n > $k)
            $low = $k + 1;
        else
            return;
    }
}

function randomizedPartition(array &$a, int $low, int $high): int {
    swap($a, $low + random_int(0, $high - $low - 1), $high - 1);
    $separator = $a[$high - 1];
    $i = $low - 1;
    for ($j = $low; $j < $high; $j++)
      if ($a[$j] <= $separator)
          swap($a, ++$i, $j);
    return $i;
}

function swap(array &$a, int $i, int $j) {
    $t = $a[$i];
    $a[$i] = $a[$j];
    $a[$j] = $t;
}