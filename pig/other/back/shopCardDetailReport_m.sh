#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

###原始日志路径
input_channel_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_sc
input_area_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_sa
#卡片静态数据
input_shopcard_path=/user/datum/uhuibao/middle/ttyh/${date}/static_shopcard
#统计日志数据(h5生成的日志)
input_shopcard_log_path=/user/datum/uhuibao/clean/ttyh/${former}/shop

###结果输出路径
out_result_path=/user/datum/uhuibao/report_month/ttyh/${former}/shopcard

#附加时间参数
static_log_time=${former}

#start

pig -p input_channel_path=${input_channel_path} -p input_area_path=${input_area_path} -p input_shopcard_path=${input_shopcard_path} -p input_shopcard_log_path=${input_shopcard_log_path} -p out_result_path=${out_result_path} -p static_log_time=${static_log_time} ${local_pig_script}/shopCardDetailReport_m.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;

