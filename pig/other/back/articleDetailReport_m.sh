#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

###原始日志路径
##4类分组数据(非简单数据)
input_type_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_zt
input_shop_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_zs
input_channel_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_zc
input_area_path=/user/datum/uhuibao/middle/ttyh/${date}/grp_za
#资讯静态数据(资讯Id,资讯名称)
input_article_path=/user/datum/uhuibao/middle/ttyh/${date}/static_article
#统计日志数据(h5生成的日志)
input_log_article_path=/user/datum/uhuibao/clean/ttyh/${former}/article
input_log_collect_path=/user/datum/uhuibao/clean/ttyh/${former}/collect
input_log_share_path=/user/datum/uhuibao/clean/ttyh/${former}/share



###日志输出
#资讯日志输出路径
out_result=/user/datum/uhuibao/report_month/ttyh/${former}/article


#start

pig -p input_type_path=${input_type_path} -p input_shop_path=${input_shop_path} -p input_channel_path=${input_channel_path} -p input_area_path=${input_area_path} -p input_article_path=${input_article_path} -p input_log_article_path=${input_log_article_path} -p input_log_collect_path=${input_log_collect_path} -p input_log_share_path=${input_log_share_path} -p out_result=${out_result} ${local_pig_script}/articleDetailReport_m.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;
