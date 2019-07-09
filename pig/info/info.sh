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
local_pig_script=/home/hadoop/pig_exe/pig_script/info

#hdfs日志路径
hdf_path=hdfs://h1:9000/data/logs/orderlog/${former}
in_hdf_path_data=${hdf_path}/*

#判断日志输入路径是否存在
hadoop fs -test -e $hdf_path
if [ $? -eq 0 ] ;then
	echo '日志输入路径存在'
else
	echo '日志输入路径是不存在,结束执行->'$hdf_path
	exit 1
fi

#清洗后输出路径<1>业务访问数据<2>业务访问响应时间
out_clean_hdf_path1=hdfs://h1:9000/data/clean/infolog/business/${former}/
out_clean_hdf_path2=hdfs://h1:9000/data/clean/infolog/businessTime/${former}/

#清洗后统计日志<1>业务访问数据<2>业务访问响应时间
out_result_hdf_path=hdfs://h1:9000/statistics/info/business/${former}/

#删除对应输出路径
hadoop fs -test -e $out_clean_hdf_path1
if [ $? -eq 0 ] ;then
    hadoop fs -rm -r $out_clean_hdf_path1
    echo '日志输出路径已存在,删除已存在的路径->'$out_clean_hdf_path1
fi
hadoop fs -test -e $out_clean_hdf_path2
if [ $? -eq 0 ] ;then
    hadoop fs -rm -r $out_clean_hdf_path2
    echo '日志输出路径已存在,删除已存在的路径->'$out_clean_hdf_path2
fi
hadoop fs -test -e $out_result_hdf_path1
if [ $? -eq 0 ] ;then
    hadoop fs -rm -r $out_result_hdf_path1
    echo '日志输出路径已存在,删除已存在的路径->'$out_result_hdf_path1
fi
hadoop fs -test -e $out_result_hdf_path2
if [ $? -eq 0 ] ;then
    hadoop fs -rm -r $out_result_hdf_path2
    echo '日志输出路径已存在,删除已存在的路径->'$out_result_hdf_path2
fi


echo "-----------开始执行日期former="$former
#执行
#start

pig -p in_hdf_path_data=${in_hdf_path_data} -p out_clean_hdf_path1=${out_clean_hdf_path1} -p out_clean_hdf_path2=${out_clean_hdf_path2} -p out_result_hdf_path=${out_result_hdf_path} ${local_pig_script}/info.pig


#for i in {11..20};do sh ttyhClean.sh 2018$i;done;

