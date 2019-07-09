#!/bin/bash
##date
source /etc/profile
date=$(date +"%Y-%m-%d" -d "$1");
date_format=${date//-/}

if [ ! -n "$1" ] ;then
    echo "请输入处理的中间数据日期,格式如:2016-11-11"
    exit
fi
echo ""
echo "处理的中间数据日期为$date==============================================$date_format"
echo ""


#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe

######################################资讯####################################
#资讯静态数据日志输入路径
input_static_article_data=/user/datum/uhuibao/tmp/data/ttyh/relation/article_type${date_format}.txt
#判断输入hdfs是否存在
hadoop fs -test -e ${input_static_article_data}
if [ $? -eq 1 ]
	then
	echo "资讯静态数据不存在"${input_static_article_data}
        exit
fi
#资讯静态数据日志输出路径（非简单数据）
out_article_type_group_data=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_article_type
#判断输出hdfs是否存在
hadoop fs -test -e ${out_article_type_group_data}
if [ $? -eq 0 ]
	then
	echo "资讯静态中间数据已存在"${out_article_type_group_data}
	exit
fi

######################################卡券####################################
#卡券静态数据日志输入路径
input_static_ticket_data=/user/datum/uhuibao/tmp/data/ttyh/relation/ticket_type${date_format}.txt
#判断输入hdfs是否存在
hadoop fs -test -e ${input_static_ticket_data}
if [ $? -eq 1 ]
	then
	echo "卡券静态数据不存在"${input_static_ticket_data}
        exit
fi
#卡券静态数据日志输出路径（非简单数据）
out_ticket_type_group_data=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_ticket_type
#判断输出hdfs是否存在
hadoop fs -test -e ${out_ticket_type_group_data}
if [ $? -eq 0 ]
	then
	echo "卡券静态中间数据已存在"${out_ticket_type_group_data}
	exit
fi


######################################淘流量####################################
#任务类型属性静态数据日志输入路径
input_static_tasktypeproperty_data=/user/datum/uhuibao/tmp/data/ttyh/relation/task_type_property${date_format}.txt
#判断输入hdfs是否存在
hadoop fs -test -e ${input_static_tasktypeproperty_data}
if [ $? -eq 1 ]
	then
	echo "任务类型属性静态数据不存在"${input_static_tasktypeproperty_data}
        exit
fi
#任务类型组静态数据日志输出路径（非简单数据）
output_static_tasktype_data=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_task_type
#任务属性组静态数据日志输出路径（非简单数据）
output_static_taskproperty_data=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_task_property
#清洗得到的静态任务类型表
output_static_type_data=/user/datum/uhuibao/middle/ttyh/static/${date}/tasktype
#清洗得到的静态任务属性表
output_static_property_data=/user/datum/uhuibao/middle/ttyh/static/${date}/taskproperty
#判断输出hdfs是否存在
hadoop fs -test -e ${output_static_tasktype_data}
if [ $? -eq 0 ]
	then
	echo "任务类型组数据已存在"${output_static_tasktype_data}
	exit
fi
hadoop fs -test -e ${output_static_taskproperty_data}
if [ $? -eq 0 ]
	then
	echo "任务属性组数据已存在"${output_static_taskproperty_data}
	exit
fi
hadoop fs -test -e ${output_static_type_data}
if [ $? -eq 0 ]
	then
	echo "静态任务类型数据已存在"${output_static_type_data}
	exit
fi
hadoop fs -test -e ${output_static_property_data}
if [ $? -eq 0 ]
	then
	echo "静态任务属性数据已存在"${output_static_property_data}
	exit
fi
#######
#任务类别静态数据日志输入路径
input_static_taskcategory_data=/user/datum/uhuibao/tmp/data/ttyh/relation/task_type${date_format}.txt
#判断输入hdfs是否存在
hadoop fs -test -e ${input_static_taskcategory_data}
if [ $? -eq 1 ]
	then
	echo "任务类别静态数据不存在"${input_static_taskcategory_data}
        exit
fi
#任务类别组静态数据日志输出路径（非简单数据）
output_static_taskcategory_data=/user/datum/uhuibao/middle/ttyh/group/${date}/grp_task_category
#判断输出hdfs是否存在
hadoop fs -test -e ${output_static_taskcategory_data}
if [ $? -eq 0 ]
	then
	echo "任务类别组中间数据已存在"${output_static_taskcategory_data}
	exit
fi





#start

pig -p output_static_type_data=${output_static_type_data} -p output_static_property_data=${output_static_property_data} -p input_static_article_data=${input_static_article_data} -p out_article_type_group_data=${out_article_type_group_data} -p input_static_ticket_data=${input_static_ticket_data} -p out_ticket_type_group_data=${out_ticket_type_group_data} -p input_static_tasktypeproperty_data=${input_static_tasktypeproperty_data} -p input_static_taskcategory_data=${input_static_taskcategory_data} -p output_static_tasktype_data=${output_static_tasktype_data} -p output_static_taskproperty_data=${output_static_taskproperty_data} -p output_static_taskcategory_data=${output_static_taskcategory_data}  ${local_pig_script}/ttyhMiddleGroup.pig

#for i in {11..20};do sh ttyhMiddleGroup.sh 2017$i;done;
