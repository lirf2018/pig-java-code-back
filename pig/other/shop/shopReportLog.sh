#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

#第一次清洗的商家日志路径
input_clean_shop_log=/user/datum/uhuibao/clean/ttyh/${former}/shop/*

#第二次清洗商家日志输出路径
out_clean_shop_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/shop_report_log/shop


#start

pig -p input_clean_shop_log=${input_clean_shop_log} -p out_clean_shop_log=${out_clean_shop_log}  ${local_pig_script}/shopReportLog.pig

#for i in {11..20};do sh shopReportLog.sh 2017$i;done;








