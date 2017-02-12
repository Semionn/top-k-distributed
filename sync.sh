#!/bin/bash
cur_path=$(cd "$(dirname "${BASH_SOURCE}")"; pwd -P )
cd "$cur_path"
/usr/local/bin/php src/DBSynchronizer.php >> /home/semionn/topk-server.log
