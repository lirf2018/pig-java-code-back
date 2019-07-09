#!/bin/bash
source /etc/profile
##date
#date=$(date +"%Y-%m-%d" -d "$1");
#former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe/article

##参数1：中间数据的时间
##参数2：要清洗计算是时间
input_date_middle=$(date +"%Y-%m-%d" -d "$1")
date=$input_date_middle
input_date_log=$(date +"%Y-%m-%d" -d "$2")
former=$input_date_log
echo ""
echo "非必须参数1为中间数据日期,必须参数2为清洗的日志日期"
echo ""
#如果只有一个参数,那计算都是按对等数据天的数据进行清洗计算
if [ ! -n "$1" ] ;then
    echo "输入清洗日志的日期"
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
echo "需要清洗的日志日期为"${former}


#如果只有一个参数,那计算都是按对等数据天的数据进行清洗计算




#############第一次清洗的原日志路径############
#第一次清洗的资讯日志路径
input_clean_article_log=/user/datum/uhuibao/clean/ttyh/${former}/article/*
#第一次清洗的收藏日志路径
input_clean_collect_log=/user/datum/uhuibao/clean/ttyh/${former}/collect/*
#第一次清洗的分享日志路径
input_clean_share_log=/user/datum/uhuibao/clean/ttyh/${former}/share/*
#判断输入hdfs是否存在
hadoop fs -test -e ${input_clean_article_log}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "资讯日志数据不存在"${input_clean_article_log}
        exit
fi
hadoop fs -test -e ${input_clean_collect_log}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "收藏日志数据不存在"${input_clean_collect_log}
        exit
fi
hadoop fs -test -e ${input_clean_share_log}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "分享日志数据不存在"${input_clean_share_log}
        exit
fi

##################日志输出(第二次清洗,保留有用日志)##################
#输出保留的资讯日志
out_clean_article_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/article_report_log/article
#输出保留的资讯收藏日志
out_clean_article_collect_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/article_report_log/collect
#输出保留的资讯分享日志
out_clean_article_share_log=/user/datum/uhuibao/middle/ttyh/clean2/${former}/article_report_log/share
#判断输出hdfs是否存在
hadoop fs -test -e ${out_clean_article_log}
if [ $? -eq 0 ]
	then
	echo "资讯第二次清洗结果数据已存在"${out_clean_article_log}
	exit
fi
hadoop fs -test -e ${out_clean_article_collect_log}
if [ $? -eq 0 ]
	then
	echo "收藏第二次清洗结果数据已存在"${out_clean_article_collect_log}
	exit
fi
hadoop fs -test -e ${out_clean_article_share_log}
if [ $? -eq 0 ]
	then
	echo "分享第二次清洗结果数据已存在"${out_clean_article_share_log}
	exit
fi



#start

pig -p input_clean_article_log=${input_clean_article_log} -p input_clean_collect_log=${input_clean_collect_log} -p input_clean_share_log=${input_clean_share_log} -p out_clean_article_log=${out_clean_article_log} -p out_clean_article_collect_log=${out_clean_article_collect_log} -p out_clean_article_share_log=${out_clean_article_share_log} ${local_pig_script}/articleReportLog.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;

