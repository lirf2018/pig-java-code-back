#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe/article

##参数1：中间数据的时间
##参数2：要分析计算是时间
input_date_middle=$(date +"%Y-%m-%d" -d "$1")
date=$input_date_middle
input_date_log=$(date +"%Y-%m-%d" -d "$2")
former=$input_date_log

#如果只有一个参数,那计算都是按对等数据天的数据进行分析计算
if [ ! -n "$1" ] ;then
    echo "输入分析日志的日期"
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
echo "需要分析的日志日期为"${former}


#如果只有一个参数,那计算都是按对等数据天的数据进行分析计算

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

##判断分析日志是否存在
hadoop fs -test -e ${input_type_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗中间资讯类型数据不存在"${input_type_path}
        exit
fi
hadoop fs -test -e ${input_shop_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗中间资讯店铺数据不存在"${input_shop_path}
        exit
fi
hadoop fs -test -e ${input_channel_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗中间资讯渠道数据不存在"${input_channel_path}
        exit
fi
hadoop fs -test -e ${input_area_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗中间资讯地区数据不存在"${input_area_path}
        exit
fi
hadoop fs -test -e ${input_article_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗中间资讯静态数据不存在"${input_article_path}
        exit
fi
hadoop fs -test -e ${input_log_article_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗资讯日志不存在"${input_log_article_path}
        exit
fi
hadoop fs -test -e ${input_log_collect_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗收藏日志不存在"${input_log_collect_path}
        exit
fi
hadoop fs -test -e ${input_log_share_path}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "清洗分享日志不存在"${input_log_share_path}
        exit
fi





###日志输出
#资讯日志输出路径
out_result=/user/datum/uhuibao/report_month/ttyh/${former}/article


#start

pig -p input_type_path=${input_type_path} -p input_shop_path=${input_shop_path} -p input_channel_path=${input_channel_path} -p input_area_path=${input_area_path} -p input_article_path=${input_article_path} -p input_log_article_path=${input_log_article_path} -p input_log_collect_path=${input_log_collect_path} -p input_log_share_path=${input_log_share_path} -p out_result=${out_result} ${local_pig_script}/articleDetailReport_m.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;
