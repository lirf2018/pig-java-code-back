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
echo "执行前请先删除获取日期内getcard_log下所有本地的数据getcard,order,cancel_order里对应的日期"
echo ""
echo "下载的dfs文件从: $1(包括)至$2(包括)日志"
echo ""

#本地或者线上本地
localRoot="/home/hadoop/lrfdata/data/backup/ttyh"


echo "开始获取领卡日报----领卡日志"
i=0
#循环
while(($i<100))
do
    echo ""
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束getcard"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/getcard_report_log/getcard
        localRootPath=${localRoot}/getcard_log/getcard/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 1 ]
            then
            echo "hdfs数据不存在"${hdfsRootPath}
	else
	     if [ ! -d "${localRootPath}" ]; then
		mkdir ${localRootPath}
		echo "文件夹创建成功${localRootPath}"
		#执行hadoop
		hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
		echo "hdfs文件夹下载成功${hdfsRootPath}"
	     else
		echo "本地文件夹已存在${localRootPath}"
	     fi
        fi
    fi    
    i=$(($i+1))
    echo ""
done
echo "结束获取领卡日报----领卡日志"

echo "开始获取领卡订单----领卡订单日志"
i=0
#循环
while(($i<100))
do
    echo ""
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束getcardorder"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/getcard_report_log/order
        localRootPath=${localRoot}/getcard_log/order/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 1 ]
            then
            echo "hdfs数据不存在"${hdfsRootPath}
	else
	     if [ ! -d "${localRootPath}" ]; then
		mkdir ${localRootPath}
		echo "文件夹创建成功${localRootPath}"
		#执行hadoop
		hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
		echo "hdfs文件夹下载成功${hdfsRootPath}"
	     else
		echo "本地文件夹已存在${localRootPath}"
	     fi
        fi
    fi    
    i=$(($i+1))
    echo ""
done
echo "结束获取领卡订单----订单日志"

echo "开始获取领卡取消订单----领卡取消订单日志"
i=0
#循环
while(($i<100))
do
    echo ""
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束getcardcancelorder"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/getcard_report_log/cancel_order
        localRootPath=${localRoot}/getcard_log/cancel_order/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 1 ]
            then
            echo "hdfs数据不存在"${hdfsRootPath}
	else
	     if [ ! -d "${localRootPath}" ]; then
		mkdir ${localRootPath}
		echo "文件夹创建成功${localRootPath}"
		#执行hadoop
		hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
		echo "hdfs文件夹下载成功${hdfsRootPath}"
	     else
		echo "本地文件夹已存在${localRootPath}"
	     fi
        fi
    fi    
    i=$(($i+1))
    echo ""
done
echo "结束获取领卡取消订单----取消订单日志"
