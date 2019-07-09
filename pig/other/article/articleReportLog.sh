#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig


#############第一次清洗的原日志路径############
#第一次清洗的资讯日志路径
input_clean_article_log=/user/datum/uhuibao/clean/ttyh/${former}/article/*
#第一次清洗的收藏日志路径
input_clean_collect_log=/user/datum/uhuibao/clean/ttyh/${former}/collect/*
#第一次清洗的分享日志路径
input_clean_share_log=/user/datum/uhuibao/clean/ttyh/${former}/share/*


##################日志输出(第二次清洗,保留有用日志)##################
#输出保留的资讯日志
out_clean_article_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/article_report_log/article
#输出保留的资讯收藏日志
out_clean_article_collect_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/article_report_log/collect
#输出保留的资讯分享日志
out_clean_article_share_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/article_report_log/share

#start

pig -p input_clean_article_log=${input_clean_article_log} -p input_clean_collect_log=${input_clean_collect_log} -p input_clean_share_log=${input_clean_share_log} -p out_clean_article_log=${out_clean_article_log} -p out_clean_article_collect_log=${out_clean_article_collect_log} -p out_clean_article_share_log=${out_clean_article_share_log} ${local_pig_script}/articleReportLog.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;

