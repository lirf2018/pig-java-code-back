#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

#第一次清洗的服务日志路径
input_clean_service_log=/user/datum/uhuibao/clean/ttyh/${former}/service/*

#第二次清洗服务日志输出路径
out_clean_service_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/service_report_log/service


#start

pig -p input_clean_service_log=${input_clean_service_log} -p out_clean_service_log=${out_clean_service_log}  ${local_pig_script}/serviceReportLog.pig

#for i in {11..20};do sh serviceReportLog.sh 2017$i;done;








