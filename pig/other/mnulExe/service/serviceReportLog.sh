#!/bin/bash
source /etc/profile
##date
#date=$(date +"%Y-%m-%d" -d "$1");
#former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe/service

##参数1：清洗日志数据的时间
input_date_log=$(date +"%Y-%m-%d" -d "$1")
former=$input_date_log

#
if [ ! -n "$1" ] ;then
    echo "输入清洗日志的日期"
    exit
fi


echo""
echo "需要清洗的日志日期为============================"${former}
echo""


#第一次清洗的服务日志路径
input_clean_service_log=/user/datum/uhuibao/clean/ttyh/${former}/service/*
#判断输入hdfs是否存在
hadoop fs -test -e ${input_clean_service_log}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "服务日志数据路径不存在"${input_clean_service_log}
        exit
fi

#第二次清洗服务日志输出路径
out_clean_service_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/service_report_log/service
#判断输出hdfs是否存在
hadoop fs -test -e ${out_clean_service_log}
if [ $? -eq 0 ]
	then
	echo "服务第二次清洗结果数据已存在"${out_clean_service_log}
	exit
fi


#start

pig -p input_clean_service_log=${input_clean_service_log} -p out_clean_service_log=${out_clean_service_log}  ${local_pig_script}/serviceReportLog.pig

#for i in {11..20};do sh serviceReportLog.sh 2017$i;done;








