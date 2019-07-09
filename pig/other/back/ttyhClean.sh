#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");
former2=$(date +"%y-%m" -d "$1");
filePre=$(date +"%d" -d "$1-1day");

#######测试(手动执行的时候打开并输入对应的日期)
#filePre=$1
#former=2016-12-${filePre}
#former2=16-12

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

###原始日志路径
input_log_path=/user/datum/uhuibao/tmp/data/ttyh/${former2}/ttyh_${filePre}*

###日志输出
#资讯日志输出路径
output_article=/user/datum/uhuibao/clean/ttyh/${former}/article
#商家日志输出路径
output_shop=/user/datum/uhuibao/clean/ttyh/${former}/shop
#优惠券日志输出路径
output_ticket=/user/datum/uhuibao/clean/ttyh/${former}/ticket
#服务日志输出路径
output_service=/user/datum/uhuibao/clean/ttyh/${former}/service
#资讯收藏输出路径
output_collect=/user/datum/uhuibao/clean/ttyh/${former}/collect
#微信分享输出路径
output_share=/user/datum/uhuibao/clean/ttyh/${former}/share
#淘流量输出路径
output_taoflow=/user/datum/uhuibao/clean/ttyh/${former}/taoflow

#start

pig -p input_log_path=${input_log_path} -p output_article=${output_article} -p output_shop=${output_shop} -p output_ticket=${output_ticket} -p output_service=${output_service} -p output_collect=${output_collect} -p output_share=${output_share} -p output_taoflow=${output_taoflow} ${local_pig_script}/ttyhClean.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;
