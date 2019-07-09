#!/bin/bash
source /etc/profile

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe/taoflow

##参数1：中间数据的时间
##参数2：要清洗计算是时间
input_date_middle=$(date +"%Y-%m-%d" -d "$1")
date=$input_date_middle
input_date_log=$(date +"%Y-%m-%d" -d "$2")
former=$input_date_log
date_format=${input_date_middle//-/}

echo ""
echo "非必须参数1为中间数据日期,必须参数2为清洗的日志日期"
echo ""
#如果只有一个参数,那计算都是按对等数据天的数据进行清洗计算
if [ ! -n "$1" ] ;then
    echo "输入清洗日志的日期"
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
echo "需要清洗的日志日期为"${former}


#如果只有一个参数,那计算都是按对等数据天的数据进行清洗计算

##########################中间数据输入路径(非简单数据)依次：任务类型|任务属性|任务类别###############################
input_grp_tasktype_path=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_task_type
input_grp_taskproperty_path=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_task_property
input_grp_taskcategory_path=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_task_category

#########################任务静态表(为了得到任务总份数,开始时间结束时间)
input_static_task_path=/user/datum/uhuibao/tmp/data/ttyh/static/task${date_format}.txt


#########################第一次清洗的淘流量数据输入
input_taoflw_log_path=/user/datum/uhuibao/clean/ttyh/${former}/taoflow/*

#########################第一次清洗的分享数据输入
input_clean_share_log=/user/datum/uhuibao/clean/ttyh/${former}/share/*

#判断输入hdfs是否存在
hadoop fs -test -e ${input_grp_tasktype_path}
if [ $? -eq 1 ]
	then
	echo "任务类型分组数据不存在"${input_grp_tasktype_path}
        exit
fi
hadoop fs -test -e ${input_grp_taskproperty_path}
if [ $? -eq 1 ]
	then
	echo "任务属性分组数据不存在"${input_grp_taskproperty_path}
        exit
fi
hadoop fs -test -e ${input_grp_taskcategory_path}
if [ $? -eq 1 ]
	then
	echo "任务类别分组数据不存在"${input_grp_taskcategory_path}
        exit
fi
hadoop fs -test -e ${input_static_task_path}
if [ $? -eq 1 ]
	then
	echo "任务静态表数据不存在"${input_static_task_path}
        exit
fi
hadoop fs -test -e ${input_taoflw_log_path}
if [ $? -eq 1 ]
	then
	echo "第一次清洗的淘流量数据不存在"${input_taoflw_log_path}
        exit
fi
hadoop fs -test -e ${input_clean_share_log}
if [ $? -eq 1 ]
	then
	echo "第一次清洗的分享数据不存在"${input_clean_share_log}
        exit
fi

#########################第二次清洗的淘流量输出
out_clean2_taoflow_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/taoflow_report_log/taoflow

#########################第二次清洗的淘流量分享输出
out_clean2_taoflowshare_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/taoflow_report_log/share

#判断输出hdfs是否存在
hadoop fs -test -e ${out_clean2_taoflow_log}
if [ $? -eq 0 ]
	then
	echo "第二次清洗的淘流量数据已存在"${out_clean2_taoflow_log}
	exit
fi
hadoop fs -test -e ${out_clean2_taoflowshare_log}
if [ $? -eq 0 ]
	then
	echo "第二次清洗的淘流量分享数据已存在"${out_clean2_taoflowshare_log}
	exit
fi

#start

pig -p input_grp_tasktype_path=${input_grp_tasktype_path} -p input_grp_taskproperty_path=${input_grp_taskproperty_path} -p input_grp_taskcategory_path=${input_grp_taskcategory_path} -p input_taoflw_log_path=${input_taoflw_log_path} -p input_clean_share_log=${input_clean_share_log} -p out_clean2_taoflow_log=${out_clean2_taoflow_log} -p out_clean2_taoflowshare_log=${out_clean2_taoflowshare_log} -p input_static_task_path=${input_static_task_path} ${local_pig_script}/taoFlowreportLog.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;















