#!/bin/bash
cur_path=$(cd "$(dirname "${BASH_SOURCE}")"; pwd -P )
cd "$cur_path"
php src/DBSynchronizer.php >> $HOME/topk-server.log
