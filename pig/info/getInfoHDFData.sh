#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

if [ ! -n "$1" ] ;then
    former=$(date +"%Y-%m-%d" -d "$1-1day");
else
    former=$1;
fi
echo "-----------执行获取HDFS   info数据日期former="${former}

#本地路径
local_root_path=/data/dataHadoop/info

#hdfs数据路径
hdfs_info_root=hdfs://h1:9000/statistics/info
hdfs_business_path=${hdfs_info_root}/business/${former}/p*

#创建本地路径
rm -rf ${local_root_path}/business/${former}
mkdir ${local_root_path}/business/${former}


#保存路径
hadoop fs -get ${hdfs_business_path} ${local_root_path}/business/${former}/


