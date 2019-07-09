#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1+1day");
former=$(date +"%Y-%m-%d" -d "$1");
dateorder=$(date +"%Y" -d "$1");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe/getcard


if [ ! -n "$1" ] ;then
    echo "输入清洗日志的日期"
    exit
fi

echo ""
echo "需要清洗的日志日期为"${former}
echo "订单数据日期为"${date}
echo ""

#领卡日志路径
input_getcard_log=/user/datum/uhuibao/tmp/data/ttyh/cardpage/${former}/*
#判断输入hdfs是否存在
hadoop fs -test -e ${input_getcard_log}
if [ $? -eq 1 ]
	then
	echo "领卡日志路径数据hdfs路径不存在"${input_getcard_log}
        exit	
fi


#第二次清洗领卡日志输出路径
out_clean_getcard_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/getcard_report_log/getcard
#判断输入hdfs是否存在
hadoop fs -test -e ${out_clean_getcard_log}
if [ $? -eq 0 ]
	then
	echo "第二次清洗领卡日志输出数据hdfs路径已存在"${out_clean_getcard_log}
        exit	
fi


#########订单系统库数据2017-03-16里面数据为15号订单数据
input_ordersysdb_log=/user/datum/uhuibao/tmp/data/ttyh/orderdb/${dateorder}/order_${date}.txt
#判断输入hdfs是否存在
hadoop fs -test -e ${input_ordersysdb_log}
if [ $? -eq 1 ]
	then
	echo "订单系统库数据hdfs路径不存在"${input_ordersysdb_log}
        exit	
fi

#领卡订单输出路径
out_order_path=/user/datum/uhuibao/middle/ttyh/clean2/${former}/getcard_report_log/order
hadoop fs -test -e ${out_order_path}
if [ $? -eq 0 ]
	then
	echo "领卡订单输出路径数据hdfs路径已存在"${out_order_path}
        exit	
fi

#领卡已退款已取消订单输出路径
out_cancelorder_path=/user/datum/uhuibao/middle/ttyh/clean2/${former}/getcard_report_log/cancel_order
hadoop fs -test -e ${out_cancelorder_path}
if [ $? -eq 0 ]
	then
	echo "领卡已退款已取消订单输出数据hdfs路径已存在"${out_cancelorder_path}
        exit	
fi


#start

pig -p input_getcard_log=${input_getcard_log} -p out_clean_getcard_log=${out_clean_getcard_log} -p input_ordersysdb_log=${input_ordersysdb_log} -p out_order_path=${out_order_path} -p out_cancelorder_path=${out_cancelorder_path} ${local_pig_script}/getCardReportLog.pig




#for i in {11..20};do sh serviceReportLog.sh 2017$i;done;








