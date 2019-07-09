#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

##参数1：中间数据的时间
##参数2：要分析计算是时间
input_date_middle=$(date +"%Y-%m-%d" -d "$1")
date=$input_date_middle
input_date_log=$(date +"%Y-%m-%d" -d "$2")
former=$input_date_log

#如果只有一个参数,那计算都是按对等数据天的数据进行分析计算
if [ ! -n "$1" ] ;then
    echo "输入分析日志的日期"
    exit
else
    echo ""
fi

if [ ! -n "$2" ] ;then
    date=$(date +"%Y-%m-%d" -d "$1+1day")
    former=$(date +"%Y-%m-%d" -d "$1")
else
    echo ""
fi

echo "中间数据日期为"${date}
echo "需要分析的日志日期为"${former}

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe/shopcard

###原始日志路径
input_channel_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_sc
input_area_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_sa
#卡片静态数据
input_shopcard_path=/user/datum/uhuibao/middle/ttyh/${date}/static_shopcard

#判断输入数据是否存在
hadoop fs -test -e ${input_channel_path}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "输入数据不存在"${input_channel_path}
        exit
fi

hadoop fs -test -e ${input_area_path}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "输入数据不存在"${input_area_path}
        exit
fi

hadoop fs -test -e ${input_shopcard_path}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "输入数据不存在"${input_shopcard_path}
        exit
fi

#统计日志数据(h5生成的日志)
input_shopcard_log_path=/user/datum/uhuibao/clean/ttyh/${former}/shop

hadoop fs -test -e ${input_shopcard_log_path}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "分析的日志数据不存在"${input_shopcard_log_path}
	exit
fi

###结果输出路径
out_result_path=/user/datum/uhuibao/report_month/ttyh/${former}/shopcard

#判断结果数据是否存在
hadoop fs -test -e ${out_result_path}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_result_path}
	exit
else
	echo ""
fi

#附加时间参数
static_log_time=${former}

#start

pig -p input_channel_path=${input_channel_path} -p input_area_path=${input_area_path} -p input_shopcard_path=${input_shopcard_path} -p input_shopcard_log_path=${input_shopcard_log_path} -p out_result_path=${out_result_path} -p static_log_time=${static_log_time} ${local_pig_script}/shopCardDetailReport_m.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;

