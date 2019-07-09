#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");
date_format=${date//-/}

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

##########卡券中间数据路径(当天)
input_grp_ticket_types_path=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_ticket_type/*
####卡券静态数据日志输入路径(当天)
input_static_ticket_data=/user/datum/uhuibao/tmp/data/ttyh/relation/ticket_type${date_format}.txt
####卡券第一次清洗后的路径(前一天)
input_ticket_clean_log_path=/user/datum/uhuibao/clean/ttyh/${former}/ticket/*
####卡券第二清洗后的路径(前一天)
out_ticket_clean_log_path=/user/datum/uhuibao/middle/ttyh/clean2/${former}/ticket_report_log/ticket


#start

pig -p input_grp_ticket_types_path=${input_grp_ticket_types_path} -p input_static_ticket_data=${input_static_ticket_data} -p input_ticket_clean_log_path=${input_ticket_clean_log_path} -p out_ticket_clean_log_path=${out_ticket_clean_log_path}  ${local_pig_script}/ticketReportLog.pig

#for i in {11..20};do sh ticketReportLog.sh 2017$i;done;

