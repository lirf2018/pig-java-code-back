#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");
former2=$(date +"%y-%m" -d "$1");
filePre=$(date +"%d" -d "$1-1day");

#####测试(手动执行的时候打开并输入对应的日期)
#filePre=12
#former=2016-12-${filePre}
#former2=16-12

#判断是否存在输入日志
###原始日志路径
input_log_path=/user/datum/uhuibao/tmp/data/ttyh/${former2}/ttyh_${filePre}*
echo $input_log_path;

hadoop fs -test -e ${input_log_path}
if [ $? -eq 0 ]; then
    echo "log exists!"
    sh /home/hadoop/lrfdata/pig/ttyhClean.sh
else
    echo "log not exists!"
fi
