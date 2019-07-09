#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

now=$(date +"%Y%m%d" -d "$1");


#######上传本地文件到hdfs中
###上传h5静态数据
#需要上传的日志路径
from_path=/home/hadoop/lrfdata/share/article
#上传到的日志路径
h5_article_file=/user/datum/uhuibao/tmp/data/ttyh/static/article

hadoop fs -test -e ${h5_article_file}
if [ $? -eq 0 ]; then
    echo "log exists!"
    hadoop fs -rm -r ${h5_article_file}/*
else
    echo "log not exists!"
fi
hadoop fs -put ${from_path}/*${now}.txt ${h5_article_file}


#start


#for i in {11..20};do sh ttyhClean.sh 2017$i;done;
