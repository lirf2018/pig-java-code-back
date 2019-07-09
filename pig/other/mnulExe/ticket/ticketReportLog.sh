#!/bin/bash
source /etc/profile
##date
#date=$(date +"%Y-%m-%d" -d "$1");
#former=$(date +"%Y-%m-%d" -d "$1-1day");


#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe/ticket

##参数1：中间数据的时间
##参数2：要清洗计算是时间
input_date_middle=$(date +"%Y-%m-%d" -d "$1")
date=$input_date_middle
date_format=${date//-/}
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
    date_format=${date//-/}
    former=$(date +"%Y-%m-%d" -d "$1")
else
    echo ""
fi

echo "中间数据日期为"${date}
echo "需要清洗的日志日期为"${former}


#如果只有一个参数,那计算都是按对等数据天的数据进行清洗计算






##########卡券中间数据路径(当天)
input_grp_ticket_types_path=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_ticket_type/*
####卡券静态数据日志输入路径(当天)为了得到卡券的  库存`开始时间`结束时间`
input_static_ticket_data=/user/datum/uhuibao/tmp/data/ttyh/relation/ticket_type${date_format}.txt
####卡券第一次清洗后的路径(前一天)
input_ticket_clean_log_path=/user/datum/uhuibao/clean/ttyh/${former}/ticket/*
#判断输入hdfs是否存在
hadoop fs -test -e ${input_grp_ticket_types_path}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "卡券中间数据hdfs路径不存在"${input_grp_ticket_types_path}
        exit
fi
hadoop fs -test -e ${input_static_ticket_data}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "卡券关联的静态数据hdfs路径不存在"${input_static_ticket_data}
        exit
fi
hadoop fs -test -e ${input_ticket_clean_log_path}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "第一次清洗的卡券数据hdfs路径不存在"${input_ticket_clean_log_path}
        exit
fi

####卡券第二清洗后的路径(前一天)
out_ticket_clean_log_path=/user/datum/uhuibao/middle/ttyh/clean2/${former}/ticket_report_log/ticket
#判断输出hdfs是否存在
hadoop fs -test -e ${out_ticket_clean_log_path}
if [ $? -eq 0 ]
	then
	echo "卡券第二次清洗结果数据已存在"${out_ticket_clean_log_path}
	exit
fi

#start

pig -p input_grp_ticket_types_path=${input_grp_ticket_types_path} -p input_static_ticket_data=${input_static_ticket_data} -p input_ticket_clean_log_path=${input_ticket_clean_log_path} -p out_ticket_clean_log_path=${out_ticket_clean_log_path}  ${local_pig_script}/ticketReportLog.pig

#for i in {11..20};do sh ticketReportLog.sh 2017$i;done;

