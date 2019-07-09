#!/bin/bash
source /etc/profile
##  突发事故处理(一次处理几天数据)  需要指定hdf的输入输出路径

if [ ! -n "$1" ] ;then
    echo "-----------缺少日志输入路径,如/log"
    exit 1;
fi

#pig脚本路径
local_pig_script=/home/hadoop/pig_exe/pig_script/chain

#hdfs日志路径
hdf_path=hdfs://h1:9000$1
in_hdf_path_data=${hdf_path}/*

echo "---->日志路径:"$in_hdf_path_data

#判断日志输入路径是否存在
hadoop fs -test -e $hdf_path
if [ $? -eq 0 ] ;then
	echo '日志输入路径存在'
else
	echo '日志输入路径是不存在,结束执行->'$hdf_path
	exit 1
fi

#统一处理名称
out_name=accident

#清洗后输出路径
out_clean_hdf_path=hdfs://h1:9000/data/clean/chainlog/${out_name}/
#清洗后统计日志
out_st_hdf_path=hdfs://h1:9000/statistics/chainlog_business/${out_name}/

#删除对应输出路径
hadoop fs -test -e $out_clean_hdf_path
if [ $? -eq 0 ] ;then
    echo '日志输出路径已存在,开始删除已存在的路径->'$out_clean_hdf_path
    hadoop fs -rm -r $out_clean_hdf_path
fi
hadoop fs -test -e $out_st_hdf_path
if [ $? -eq 0 ] ;then
    echo '日志输出路径已存在,开始删除已存在的路径->'$out_st_hdf_path
    hadoop fs -rm -r $out_st_hdf_path
fi

echo "---------->清洗后输出路径:"$out_clean_hdf_path
echo "---------->清洗后输出路径:"$out_st_hdf_path

echo "-----------开始执行"

#执行
#start


pig  -p in_hdf_path_data=${in_hdf_path_data} -p out_clean_hdf_path=${out_clean_hdf_path} -p out_st_hdf_path=${out_st_hdf_path}  ${local_pig_script}/clearChainLog.pig

#for i in {11..20};do sh ttyhClean.sh 2018$i;done;