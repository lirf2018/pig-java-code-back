#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");
dateFormat=$(date +"%Y%m%d" -d "$1");

#######上传本地文件到hdfs中
###上传h5静态数据
#需要上传的日志路径
local_path=/data/syncfile/ttyh
##############上传到hdfs的日志路径
#日志关系类数据
h5_relation_file=/user/datum/uhuibao/tmp/data/ttyh/relation
#表静态数据
h5_static_file=/user/datum/uhuibao/tmp/data/ttyh/static

hadoop fs -put ${local_path}/relation/*${dateFormat}.txt ${h5_relation_file}
hadoop fs -put ${local_path}/static/*${dateFormat}.txt ${h5_static_file}




#其它数据
##########################################订单数据格式############################
dateyear=$(date +"%Y" -d "$1");
#判断输入hdfs是否存在
orderdb_hdfs=/user/datum/uhuibao/tmp/data/ttyh/orderdb/${dateyear}
hadoop fs -test -e ${orderdb_hdfs}
if [ $? -eq 1 ]
	then
	hadoop fs -mkdir ${orderdb_hdfs}
fi
#本地路径
orderdb_local=${local_path}/order_mange
hadoop fs -put ${orderdb_local}/order_${date}.txt ${orderdb_hdfs}


#start


#for i in {11..20};do sh ttyhClean.sh 2017$i;done;