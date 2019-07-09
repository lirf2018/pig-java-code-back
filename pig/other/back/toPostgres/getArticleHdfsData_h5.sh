#!/bin/bash
####从hdfs获取h5分析结果
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1")
former=$(date +"%Y-%m-%d" -d "$1-1day")
#lamer=$(date +"%Y-%m-%d" -d "$1+1day")

rootPath_hdfs=/user/datum/uhuibao
rootPath_local=/home/hadoop/lrfdata/data/backup/ttyh

####################################资讯月报####################################
echo "开始从hdfs获取资讯月报"$former"数据"
echo "开始从hdfs获取静态"$date"数据"

#########资讯月报hdfs路径
article_report_month_hdfs=${rootPath_hdfs}/report_month/ttyh/${former}/article/*
hadoop fs -test -e ${article_report_month_hdfs}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "月报数据不存在"${article_report_month_hdfs}
        exit
fi
#########资讯月报本地路径
article_report_month_local=${rootPath_local}/article/report_month/${former}
#判断是否已存在本地目录
if [ ! -d "$article_report_month_local" ]; then  
    mkdir "$article_report_month_local"
else
    echo "月报本地目录已存在"$article_report_month_local
    exit 
fi
#########静态数据hdfs路径
static_channel_hdfs=${rootPath_hdfs}/middle/ttyh/${date}/static_channel/*
static_shop_hdfs=${rootPath_hdfs}/middle/ttyh/${date}/static_shop/*
static_area_hdfs=${rootPath_hdfs}/middle/ttyh/${date}/static_area/*
static_type_hdfs=${rootPath_hdfs}/middle/ttyh/${date}/static_type/*
hadoop fs -test -e ${static_channel_hdfs}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "静态渠道数据不存在"${static_channel_hdfs}
        exit
fi
hadoop fs -test -e ${static_shop_hdfs}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "静态店铺数据不存在"${static_shop_hdfs}
        exit
fi
hadoop fs -test -e ${static_area_hdfs}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "静态地区数据不存在"${static_area_hdfs}
        exit
fi
hadoop fs -test -e ${static_type_hdfs}
if [ $? -eq 0 ]
        then
        echo ""
else
        echo "静态类型数据不存在"${static_type_hdfs}
        exit
fi
#########静态数据本地路径
static_channel_local=${rootPath_local}/table_static/channel/${date}
static_shop_local=${rootPath_local}/table_static/shop/${date}
static_area_local=${rootPath_local}/table_static/area/${date}
static_type_local=${rootPath_local}/table_static/type/${date}
#判断静态数据是否已存在本地目录
if [ ! -d "$static_channel_local" ]; then
    mkdir "$static_channel_local"
else
    echo "渠道本地目录已存在"$static_channel_local
    exit
fi
if [ ! -d "$static_shop_local" ]; then
    mkdir "$static_shop_local"
else
    echo "店铺本地目录已存在"$static_shop_local
    exit
fi
if [ ! -d "$static_area_local" ]; then
    mkdir "$static_area_local"
else
    echo "地区本地目录已存在"$static_area_local
    exit
fi
if [ ! -d "$static_type_local" ]; then
    mkdir "$static_type_local"
else
    echo "类型本地目录已存在"$static_type_local
    exit
fi


hadoop fs -get ${article_report_month_hdfs} ${article_report_month_local}
hadoop fs -get ${static_channel_hdfs} ${static_channel_local}
hadoop fs -get ${static_shop_hdfs} ${static_shop_local}
hadoop fs -get ${static_area_hdfs} ${static_area_local}
hadoop fs -get ${static_type_hdfs} ${static_type_local}
echo "成功获取资讯月报数据"$article_report_month_local
echo "成功获取渠道静态数据"$static_channel_local
echo "成功获取店铺静态数据"$static_shop_local
echo "成功获取地区静态数据"$static_area_local
echo "成功获取类型静态数据"$static_type_local








