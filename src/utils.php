<?php

function checkAndLog($functionResult, string $message) {
    if ($functionResult === false) {
        error_log($message);
    }
}

/**
 * @param int $timeFrameSize seconds
 * @return int
 */
function getCurrentTimeFrame(int $timeFrameSize)
{
    return (intval(time() / ($timeFrameSize)) - 1) * $timeFrameSize;
}
