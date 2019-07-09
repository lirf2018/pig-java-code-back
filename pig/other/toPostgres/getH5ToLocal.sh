#!/bin/bash
####从hdfs获取h5第二次清洗结果和h5静态数据
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1")
former=$(date +"%Y-%m-%d" -d "$1-1day")
dataFormat=$(date +"%Y%m%d" -d "$1");

rootPath_hdfs=${rootPath_hdfs}
rootPath_local=/data/backup/ttyh

#####################获取资讯清洗数据##########################
#本地
articlet_local_path=${rootPath_local}/article_log/article/${former}
articlet_collect_local_path=${rootPath_local}/article_log/collect/${former}
articlet_share_local_path=${rootPath_local}/article_log/share/${former}
#判断是否已存在本地目录
if [ ! -d "$articlet_local_path" ]; then  
    mkdir "$articlet_local_path"
fi
if [ ! -d "$articlet_collect_local_path" ]; then  
    mkdir "$articlet_collect_local_path"
fi
if [ ! -d "$articlet_share_local_path" ]; then  
    mkdir "$articlet_share_local_path"
fi
#hdfs
articlet_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/article_report_log/article
articlet_collect_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/article_report_log/collect
articlet_share_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/article_report_log/share
hadoop fs -get ${articlet_hdfs_path}/*  ${articlet_local_path}/
hadoop fs -get ${articlet_collect_hdfs_path}/*  ${articlet_collect_local_path}/
hadoop fs -get ${articlet_share_hdfs_path}/*  ${articlet_share_local_path}/
echo ""
#####################获取卡券清洗数据##########################
ticket_local_path=${rootPath_local}/ticket_log/ticket/${former}
if [ ! -d "$ticket_local_path" ]; then  
    mkdir "$ticket_local_path"
fi
ticket_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/ticket_report_log/ticket
hadoop fs -get ${ticket_hdfs_path}/*  ${ticket_local_path}/
echo ""
#####################获取商家清洗数据##########################
shoplog_local_path=${rootPath_local}/shop_log/shop/${former}
if [ ! -d "$shoplog_local_path" ]; then  
    mkdir "$shoplog_local_path"
fi
shoplog_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/shop_report_log/shop
hadoop fs -get ${shoplog_hdfs_path}/*  ${shoplog_local_path}/
echo ""
#####################获取服务清洗数据##########################
service_local_path=${rootPath_local}/service_log/service/${former}
if [ ! -d "$service_local_path" ]; then  
    mkdir "$service_local_path"
fi
service_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/service_report_log/service
hadoop fs -get ${service_hdfs_path}/*  ${service_local_path}/
echo ""
#####################获取淘流量清洗数据##########################
#local
taoflow_local_path=${rootPath_local}/taoflow_log/taoflow/${former}
taoflow_share_local_path=${rootPath_local}/taoflow_log/share/${former}
taoflow_statistics_local_path=${rootPath_local}/taoflow_log/statistics/${former}
taoflow_type_local_path=${rootPath_local}/taoflow_log/static_task_type/${former}
taoflow_property_local_path=${rootPath_local}/taoflow_log/static_task_property/${former}
if [ ! -d "$taoflow_local_path" ]; then  
    mkdir "$taoflow_local_path"
fi
if [ ! -d "$taoflow_share_local_path" ]; then  
    mkdir "$taoflow_share_local_path"
fi
if [ ! -d "$taoflow_statistics_local_path" ]; then  
    mkdir "$taoflow_statistics_local_path"
fi
if [ ! -d "$taoflow_type_local_path" ]; then  
    mkdir "$taoflow_type_local_path"
fi
if [ ! -d "$taoflow_property_local_path" ]; then  
    mkdir "$taoflow_property_local_path"
fi
echo ""
#hdfs
taoflow_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/taoflow_report_log/taoflow
taoflow_share_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${former}/taoflow_report_log/share
taoflow_statistics_hdfs_path=${rootPath_hdfs}/tmp/data/ttyh/relation/task_record${dataFormat}.txt
taoflow_type_hdfs_path=${rootPath_hdfs}/middle/ttyh/static/${date}/tasktype
taoflow_property_hdfs_path=${rootPath_hdfs}/middle/ttyh/static/${date}/taskproperty
hadoop fs -get ${taoflow_hdfs_path}/*  ${taoflow_local_path}/
hadoop fs -get ${taoflow_share_hdfs_path}/*  ${taoflow_share_local_path}/
hadoop fs -get ${taoflow_statistics_hdfs_path}  ${taoflow_statistics_local_path}/
hadoop fs -get ${taoflow_type_hdfs_path}/*  ${taoflow_type_local_path}/
hadoop fs -get ${taoflow_property_hdfs_path}/*  ${taoflow_property_local_path}/
echo ""
#####################获取领卡清洗数据##########################
#本地
getcard_local_path=${rootPath_local}/getcard_log/getcard/${getDate}
getcard_order_local_path=${rootPath_local}/getcard_log/order/${getDate}
getcard_cancelorder_local_path=${rootPath_local}/getcard_log/cancel_order/${getDate}
if [ ! -d "$getcard_local_path" ]; then  
    mkdir "$getcard_local_path"
fi
if [ ! -d "$getcard_order_local_path" ]; then  
    mkdir "$getcard_order_local_path"
fi
if [ ! -d "$getcard_cancelorder_local_path" ]; then  
    mkdir "$getcard_cancelorder_local_path"
fi
echo ""
#hdfs
getcard_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${getDate}/getcard_report_log/getcard
getcard_order_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${getDate}/getcard_report_log/order
getcard_cancelorder_hdfs_path=${rootPath_hdfs}/middle/ttyh/clean2/${getDate}/getcard_report_log/cancel_order
hadoop fs -get ${getcard_hdfs_path}/*  ${getcard_local_path}/
hadoop fs -get ${getcard_order_hdfs_path}/*  ${getcard_order_local_path}/
hadoop fs -get ${getcard_cancelorder_hdfs_path}/*  ${getcard_cancelorder_local_path}/

#####################获取静态数据##########################
h5_static_local_path=${rootPath_local}/table_static/${date}
if [ ! -d "$h5_static_local_path" ]; then  
    mkdir "$h5_static_local_path"
fi
h5_static_hdfs_path=${rootPath_hdfs}/tmp/data/ttyh/static
hadoop fs -get ${h5_static_hdfs_path}/*${dataFormat}.txt  ${h5_static_local_path}/