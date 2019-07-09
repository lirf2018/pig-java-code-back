#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");
date_format=${date//-/}

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

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

#########################第二次清洗的淘流量输出
out_clean2_taoflow_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/taoflow_report_log/taoflow

#########################第二次清洗的淘流量分享输出
out_clean2_taoflowshare_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/taoflow_report_log/share


#start

pig -p input_grp_tasktype_path=${input_grp_tasktype_path} -p input_grp_taskproperty_path=${input_grp_taskproperty_path} -p input_grp_taskcategory_path=${input_grp_taskcategory_path} -p input_taoflw_log_path=${input_taoflw_log_path} -p input_clean_share_log=${input_clean_share_log} -p out_clean2_taoflow_log=${out_clean2_taoflow_log} -p out_clean2_taoflowshare_log=${out_clean2_taoflowshare_log} -p input_static_task_path=${input_static_task_path} ${local_pig_script}/taoFlowreportLog.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;















