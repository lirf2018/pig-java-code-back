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
echo "-----------执行日期former="${former}


#pig脚本路径
local_pig_script=/home/hadoop/pig_exe/pig_script/chain

#hdfs日志路径
hdf_path=hdfs://h1:9000/data/logs/chainlog/${former}
in_hdf_path_data=${hdf_path}/*

#判断日志输入路径是否存在
hadoop fs -test -e $hdf_path
if [ $? -eq 0 ] ;then
	echo '日志输入路径存在'
else
	echo '日志输入路径是不存在,结束执行->'$hdf_path
	exit 1
fi

#清洗后输出路径
out_clean_hdf_path=hdfs://h1:9000/data/clean/chainlog/${former}/
#清洗后统计日志
out_st_hdf_path=hdfs://h1:9000/statistics/chainlog_business/${former}/

#删除对应输出路径
hadoop fs -test -e $out_clean_hdf_path
if [ $? -eq 0 ] ;then
    hadoop fs -rm -r $out_clean_hdf_path
    echo '日志输出路径已存在,删除已存在的路径->'$out_clean_hdf_path
fi
hadoop fs -test -e $out_st_hdf_path
if [ $? -eq 0 ] ;then
    hadoop fs -rm -r $out_st_hdf_path
    echo '日志输出路径已存在,删除已存在的路径->'$out_st_hdf_path
fi

#判断日志输出路径是否存在
#hadoop fs -test -e $logout_hdf_path
#if [ $? -eq 0 ] ;then
#	echo '日志输出路径已存在,结束执行->'$logout_hdf_path
#	exit 1
#else
#	echo '日志输入路径是不存在,开始执行->'$login_hdf_path
#fi




echo "-----------开始执行日期former="$former
#执行
#start

pig -p former=${former} -p in_hdf_path_data=${in_hdf_path_data} -p out_clean_hdf_path=${out_clean_hdf_path} -p out_st_hdf_path=${out_st_hdf_path}  ${local_pig_script}/clearChainLog.pig

#for i in {11..20};do sh ttyhClean.sh 2018$i;done;