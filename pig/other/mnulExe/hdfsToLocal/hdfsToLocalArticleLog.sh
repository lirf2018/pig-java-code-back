#!/bin/bash
echo ""

echo "只循环100天"

if [ ! -n "$1" ] ;then
    echo "请输入获取hdfs日志的开始时间(包含输入的时间),格式如:2016-11-11"
    exit
fi
if [ ! -n "$2" ] ;then
    echo "请输入获取hdfs日志的结束时间(包含输入的时间),格式如:2016-11-11"
    exit
fi

#比较日期
t1=`date -d "$1" +%s`
t2=`date -d "$2" +%s`

if [ $t1 -gt $t2 ]; then
   echo "结束日期不能小于开始日期"
   exit
fi

#参数1：包括的起始时间
#参数2：包括的截止时间

echo ""
echo "执行前请先删除获取日期内article_log下所有本地的数据article,collect,share里对应的日期"
echo ""
echo "下载的dfs文件从: $1(包括)至$2(包括)日志"
echo ""

#本地或者线上本地
localRoot="/home/hadoop/lrfdata/data/backup/ttyh"


echo "开始获取资讯日报----资讯日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束article"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/article_report_log/article
        localRootPathArticle=${localRoot}/article_log/article/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPathArticle}" ]; then
            mkdir ${localRootPathArticle}
            echo "文件夹创建成功${localRootPathArticle}"
        else
            echo "本地文件夹已存在${localRootPathArticle}程序停止"
            exit
            #rm -rf ${localRootPathArticle}
            #mkdir ${localRootPathArticle}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPathArticle}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取资讯日报----资讯日志"
echo ""

echo "开始获取资讯日报----收藏日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束collect"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/article_report_log/collect
        localRootPathCollect=${localRoot}/article_log/collect/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPathCollect}" ]; then
            mkdir ${localRootPathCollect}
            echo "文件夹创建成功${localRootPathCollect}"
        else
            echo "本地文件夹已存在${localRootPathCollect}程序停止"
            exit
            #rm -rf ${localRootPathCollect}
            #mkdir ${localRootPathCollect}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPathCollect}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取资讯日报----收藏日志"
echo ""
echo "开始获取资讯日报----分享日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束collect"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/article_report_log/share
        localRootPathShare=${localRoot}/article_log/share/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPathShare}" ]; then
            mkdir ${localRootPathShare}
            echo "文件夹创建成功${localRootPathShare}"
        else
            echo "本地文件夹已存在${localRootPathShare}程序停止"
            exit
            #rm -rf ${localRootPathShare}
            #mkdir ${localRootPathShare}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPathShare}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取资讯日报----分享日志"