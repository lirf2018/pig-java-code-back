#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

###原始日志路径
input_channel_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_tc
input_shop_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_ts
input_type_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_tt
#卡券静态数据
input_ticket_path=/user/datum/uhuibao/middle/ttyh/${date}/static_ticket
#统计日志数据(h5生成的日志)
input_ticket_log_path=/user/datum/uhuibao/clean/ttyh/${former}/ticket

###结果输出路径
out_result_path=/user/datum/uhuibao/report_month/ttyh/${former}/ticket

#附加时间参数
static_log_time=${former}

#start

pig -p input_channel_path=${input_channel_path} -p input_shop_path=${input_shop_path} -p input_type_path=${input_type_path} -p input_ticket_path=${input_ticket_path} -p input_ticket_log_path=${input_ticket_log_path} -p out_result_path=${out_result_path} -p static_log_time=${static_log_time} ${local_pig_script}/ticketDetailReport_m.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;

