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
echo "执行前请先删除获取日期内taoflow_log下所有本地的数据taoflow,share,static_task_property,static_task_type,statistics里对应的日期"
echo ""
echo "下载的dfs文件从: $1(包括)至$2(包括)日志"
echo ""

#本地或者线上本地
localRoot="/home/hadoop/lrfdata/data/backup/ttyh"


echo "开始获取淘流量日报----淘流量日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束taoflow"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/taoflow_report_log/taoflow
        localRootPath=${localRoot}/taoflow_log/taoflow/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPath}" ]; then
            mkdir ${localRootPath}
            echo "文件夹创建成功${localRootPath}"
        else
            echo "本地文件夹已存在${localRootPath}程序停止"
            exit
            #rm -rf ${localRootPath}
            #mkdir ${localRootPath}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取淘流量日报----淘流量日志"

echo ""
echo "开始获取淘流量日报----分享日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束share"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/clean2/${getDate}/taoflow_report_log/share
        localRootPath=${localRoot}/taoflow_log/share/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPath}" ]; then
            mkdir ${localRootPath}
            echo "文件夹创建成功${localRootPath}"
        else
            echo "本地文件夹已存在${localRootPath}程序停止"
            exit
            #rm -rf ${localRootPath}
            #mkdir ${localRootPath}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取淘流量日报----分享日志"


echo ""

echo "开始获取淘流量日报----统计日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束statistics"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
	j=0
	j=i+1
	getDate2=$(date +"%Y-%m-%d" -d "$1+"$j"day")
	getDateFormat=${getDate2//-/}
        hdfsRootPath=/user/datum/uhuibao/tmp/data/ttyh/relation/task_record${getDateFormat}.txt
        localRootPath=${localRoot}/taoflow_log/statistics/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPath}" ]; then
            mkdir ${localRootPath}
            echo "文件夹创建成功${localRootPath}"
        else
            echo "本地文件夹已存在${localRootPath}程序停止"
            exit
            #rm -rf ${localRootPath}
            #mkdir ${localRootPath}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取淘流量日报----统计日志"



echo ""
echo "开始获取淘流量日报----类别日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束type"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
	j=i+1
	getDate2=$(date +"%Y-%m-%d" -d "$1+"$j"day")
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/static/${getDate2}/tasktype
        localRootPath=${localRoot}/taoflow_log/static_task_type/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPath}" ]; then
            mkdir ${localRootPath}
            echo "文件夹创建成功${localRootPath}"
        else
            echo "本地文件夹已存在${localRootPath}程序停止"
            exit
            #rm -rf ${localRootPath}
            #mkdir ${localRootPath}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取淘流量日报----类别日志"


echo ""
echo "开始获取淘流量日报----属性日志"
i=0
#循环
while(($i<100))
do
    getDate=$(date +"%Y-%m-%d" -d "$1+"$i"day")
    t3_=`date -d "$getDate" +%s`
    if [ $t3_ -gt $t2 ]; then
        echo "循环结束property"
	i=10000
        #exit
    else
        echo "循环获取日期=$getDate"
	j=0
	j=i+1
	getDate2=$(date +"%Y-%m-%d" -d "$1+"$j"day")
        hdfsRootPath=/user/datum/uhuibao/middle/ttyh/static/${getDate2}/taskproperty
        localRootPath=${localRoot}/taoflow_log/static_task_property/${getDate}

        #判断hdfs路径是否存在
        hadoop fs -test -e ${hdfsRootPath}
        if [ $? -eq 0 ]
            then
            echo ""
        else
            echo "hdfs数据不存在"${hdfsRootPath}
            exit
        fi
        if [ ! -d "${localRootPath}" ]; then
            mkdir ${localRootPath}
            echo "文件夹创建成功${localRootPath}"
        else
            echo "本地文件夹已存在${localRootPath}程序停止"
            exit
            #rm -rf ${localRootPath}
            #mkdir ${localRootPath}
        fi
        #执行hadoop
        hadoop fs -get ${hdfsRootPath}/* ${localRootPath}/
        echo "hdfs文件夹下载成功"
        fi    
    i=$(($i+1))
done
echo "结束获取淘流量日报----属性日志"
