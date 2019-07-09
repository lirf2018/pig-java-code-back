#按实际要求恢复数据
#!/bin/bash
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");
ndata=$(date +"%Y%m%d" -d "$1");

#判断入参
if [ ! -n "$1" ] ;then
    echo "请输入计算清洗日志中间数据的时间,格式如:2016-11-11"
    exit
else
    echo "清洗日志中间数据的时间： ==============================$1"
fi

input_date=$(date +"%Y%m%d" -d "$1")
ndata=$input_date
date=$(date +"%Y-%m-%d" -d "$1")




#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe

###article原始日志路径
#包括(资讯,资讯名称,类别Id,类别名称,店铺id,店铺编码,店铺名称,)
input_log_path1=/user/datum/uhuibao/tmp/data/ttyh/static/article/article_shop_type${ndata}.txt
#包括(资讯,资讯名称,渠道编码,渠道名称,)
input_log_path2=/user/datum/uhuibao/tmp/data/ttyh/static/article/article_channel${ndata}.txt
#包括(资讯,资讯名称,地区编码,地区名称,)
input_log_path3=/user/datum/uhuibao/tmp/data/ttyh/static/article/article_area${ndata}.txt
###ticket原始日志路径
input_log_path4=/user/datum/uhuibao/tmp/data/ttyh/static/article/ticket_shop_channel_type${ndata}.txt
###shopcard原始日志路径
input_log_path5=/user/datum/uhuibao/tmp/data/ttyh/static/article/shop_channel_area${ndata}.txt


#判断原始日志是否存在
hadoop fs -test -e ${input_log_path1}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "清洗日志不存在"${input_log_path1}
        exit
fi

hadoop fs -test -e ${input_log_path2}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "清洗日志不存在"${input_log_path2}
        exit
fi

hadoop fs -test -e ${input_log_path3}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "清洗日志不存在"${input_log_path3}
        exit
fi

hadoop fs -test -e ${input_log_path4}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "清洗日志不存在"${input_log_path4}
        exit
fi

hadoop fs -test -e ${input_log_path5}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "清洗日志不存在"${input_log_path5}
        exit
fi



######分组保存中间数据(非简单数据)
#(中间数据)保存资讯和对应的  类别  分组的非简单数据
out_ttyh_sqldata_grp_zt=/user/datum/uhuibao/middle/ttyh/${date}/grp_zt
#(中间数据)保存资讯和对应的  商店  分组的非简单数据
out_ttyh_sqldata_grp_zs=/user/datum/uhuibao/middle/ttyh/${date}/grp_zs
#(中间数据)保存资讯和对应的  渠道code 分组的非简单数据
out_ttyh_sqldata_grp_zc=/user/datum/uhuibao/middle/ttyh/${date}/grp_zc
#(中间数据)保存资讯和对应的  地区  分组的非简单数据
out_ttyh_sqldata_grp_za=/user/datum/uhuibao/middle/ttyh/${date}/grp_za

#(中间数据)保存卡券和对应 商家 分组的非简单数据
out_ttyh_sqldata_grp_ts=/user/datum/uhuibao/middle/ttyh/${date}/grp_ts
#(中间数据)保存卡券和对应 渠道code 分组的非简单数据
out_ttyh_sqldata_grp_tc=/user/datum/uhuibao/middle/ttyh/${date}/grp_tc
#(中间数据)保存卡券和对应 类型  分组的非简单数据
out_ttyh_sqldata_grp_tt=/user/datum/uhuibao/middle/ttyh/${date}/grp_tt

#(中间数据)保存卡片和对应 渠道 分组的非简单数据
out_ttyh_sqldata_grp_sc=/user/datum/uhuibao/middle/ttyh/${date}/grp_sc
#(中间数据)保存卡片和对应 地区 分组的非简单数据
out_ttyh_sqldata_grp_sa=/user/datum/uhuibao/middle/ttyh/${date}/grp_sa

#判断中间数据是否存在
hadoop fs -test -e ${out_ttyh_sqldata_grp_zt}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_zt}
	exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_zs}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_zs}
	exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_zc}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_zc}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_za}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_za}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_ts}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_ts}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_tc}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_tc}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_tt}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_tt}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_sc}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_sc}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_grp_sa}
if [ $? -eq 0 ]
	then
	echo "中间数据已存在"${out_ttyh_sqldata_grp_sa}
        exit
else
	echo ""
fi
########静态数据输出
#输出(类别id,名称)
out_ttyh_sqldata_static_type=/user/datum/uhuibao/middle/ttyh/${date}/static_type
#输出商家(商家id,商家名称)
out_ttyh_sqldata_static_shop=/user/datum/uhuibao/middle/ttyh/${date}/static_shop
#输出渠道(渠道编码,渠道名称)
out_ttyh_sqldata_static_channel=/user/datum/uhuibao/middle/ttyh/${date}/static_channel
#输出地区(地区编码,地区名称)
out_ttyh_sqldata_static_area=/user/datum/uhuibao/middle/ttyh/${date}/static_area
#输出资讯(资讯id,资讯名称)
out_ttyh_sqldata_static_article=/user/datum/uhuibao/middle/ttyh/${date}/static_article
#输出卡券(卡券id,卡券名称,库存,开始时间,结束时间)
out_ttyh_sqldata_static_ticket=/user/datum/uhuibao/middle/ttyh/${date}/static_ticket
#输出商家卡片(卡片code,卡片名称)
out_ttyh_sqldata_static_shopcard=/user/datum/uhuibao/middle/ttyh/${date}/static_shopcard

#判断结果数据是否存在
hadoop fs -test -e ${out_ttyh_sqldata_static_type}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_ttyh_sqldata_static_type}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_static_shop}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_ttyh_sqldata_static_shop}
        exit
else
	echo ""
fi


hadoop fs -test -e ${out_ttyh_sqldata_static_channel}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_ttyh_sqldata_static_channel}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_static_area}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_ttyh_sqldata_static_area}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_static_article}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_ttyh_sqldata_static_article}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_static_ticket}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_ttyh_sqldata_static_ticket}
        exit
else
	echo ""
fi

hadoop fs -test -e ${out_ttyh_sqldata_static_shopcard}
if [ $? -eq 0 ]
	then
	echo "结果数据已存在"${out_ttyh_sqldata_static_shopcard}
        exit
else
	echo ""
fi

#start

pig -p input_log_path1=${input_log_path1} -p input_log_path2=${input_log_path2} -p input_log_path3=${input_log_path3} -p input_log_path4=${input_log_path4} -p input_log_path5=${input_log_path5} -p out_ttyh_sqldata_grp_zt=${out_ttyh_sqldata_grp_zt} -p out_ttyh_sqldata_grp_zs=${out_ttyh_sqldata_grp_zs} -p out_ttyh_sqldata_grp_zc=${out_ttyh_sqldata_grp_zc} -p out_ttyh_sqldata_grp_za=${out_ttyh_sqldata_grp_za} -p out_ttyh_sqldata_static_type=${out_ttyh_sqldata_static_type} -p out_ttyh_sqldata_static_shop=${out_ttyh_sqldata_static_shop} -p out_ttyh_sqldata_static_channel=${out_ttyh_sqldata_static_channel} -p out_ttyh_sqldata_static_area=${out_ttyh_sqldata_static_area} -p out_ttyh_sqldata_static_article=${out_ttyh_sqldata_static_article} -p out_ttyh_sqldata_grp_ts=${out_ttyh_sqldata_grp_ts} -p out_ttyh_sqldata_grp_tc=${out_ttyh_sqldata_grp_tc} -p out_ttyh_sqldata_grp_tt=${out_ttyh_sqldata_grp_tt} -p out_ttyh_sqldata_static_ticket=${out_ttyh_sqldata_static_ticket} -p out_ttyh_sqldata_grp_sc=${out_ttyh_sqldata_grp_sc} -p out_ttyh_sqldata_grp_sa=${out_ttyh_sqldata_grp_sa} -p out_ttyh_sqldata_static_shopcard=${out_ttyh_sqldata_static_shopcard} ${local_pig_script}/ttyhMiddleGroup.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;
