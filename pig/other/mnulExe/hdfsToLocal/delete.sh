#!/bin/bash
echo ""

if [ ! -n "$1" ] ;then
    echo "请输入删除的本地日志的时间,格式如:2016-11-11"
    exit
fi

localRot=/data/backup/ttyh

echo ""
echo "请输入删除的本地日志的时间为$1"


#删除资讯的
rm -rf ${localRot}/article_log/article/$1
rm -rf ${localRot}/article_log/collect/$1
rm -rf ${localRot}/article_log/share/$1

#删除领卡的
rm -rf ${localRot}/getcard_log/cancel_order/$1
rm -rf ${localRot}/getcard_log/getcard/$1
rm -rf ${localRot}/getcard_log/order/$1

#删除服务的
rm -rf ${localRot}/service_log/service/$1

#删除商家的
rm -rf ${localRot}/shop_log/shop/$1

#删除淘流量的
rm -rf ${localRot}/taoflow_log/share/$1
rm -rf ${localRot}/taoflow_log/static_task_property/$1
rm -rf ${localRot}/taoflow_log/static_task_type/$1
rm -rf ${localRot}/taoflow_log/statistics/$1
rm -rf ${localRot}/taoflow_log/taoflow/$1

#删除卡券的
rm -rf ${localRot}/ticket_log/ticket/$1

echo ""
echo "删除完成"


