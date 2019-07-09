#!/bin/bash
source /etc/profile
##date
date=$(date +"%Y-%m-%d" -d "$1");
former=$(date +"%Y-%m-%d" -d "$1-1day");

#代码路径
local_pig_script=/home/hadoop/lrfdata/pig

###原始日志路径
#包括(资讯,资讯名称,类别Id,类别名称)
input_log_path1=/tmp/data/ttyh/${former}/zts/*.sql
#包括(资讯,资讯名称,渠道编码,渠道名称)
input_log_path2=/tmp/data/ttyh/${former}/zc/*.sql
#包括(资讯,资讯名称,地区编码,地区名称)
input_log_path3=/tmp/data/ttyh/${former}/za/*.sql

###静态资讯日志输出sql
#去掉sql中的字符,输出为(资讯,资讯名称,类别Id,类别名称,店铺,店铺名称)
out_ttyh_sqldata_zts=/clean/ttyh/${former}/sqldata/articlet_type_shop
#去掉sql中的字符,输出为(资讯,资讯名称,渠道编码,渠道名称)
out_ttyh_sqldata_zc=/clean/ttyh/${former}/sqldata/articlet_channel
#去掉sql中的字符,输出为(资讯,资讯名称,地区编码,地区名称)
out_ttyh_sqldata_za=/clean/ttyh/${former}/sqldata/articlet_area

######分组保存中间数据
#(中间数据)保存资讯和对应的  类别  分组的非简单数据
out_ttyh_sqldata_grp_zt=/clean/ttyh/${former}/sqldata/grpdata/middle/zt
#(中间数据)保存资讯和对应的  商店  分组的非简单数据
out_ttyh_sqldata_grp_zs=/clean/ttyh/${former}/sqldata/grpdata/middle/zs
#(中间数据)保存资讯和对应的  渠道  分组的非简单数据
out_ttyh_sqldata_grp_zc=/clean/ttyh/${former}/sqldata/grpdata/middle/zc
#(中间数据)保存资讯和对应的  地区  分组的非简单数据
out_ttyh_sqldata_grp_za=/clean/ttyh/${former}/sqldata/grpdata/middle/za

########静态数据输出
#输出类别
out_ttyh_sqldata_static_type=/clean/ttyh/${former}/sqldata/static/type
#输出商家
out_ttyh_sqldata_static_shop=/clean/ttyh/${former}/sqldata/static/shop
#输出渠道
out_ttyh_sqldata_static_channel=/clean/ttyh/${former}/sqldata/static/channel
#输出地区
out_ttyh_sqldata_static_area=/clean/ttyh/${former}/sqldata/static/area
#输出资讯
out_ttyh_sqldata_static_article=/clean/ttyh/${former}/sqldata/static/article

#start

pig -p input_log_path1=${input_log_path1} -p input_log_path2=${input_log_path2} -p input_log_path3=${input_log_path3} -p out_ttyh_sqldata_zts=${out_ttyh_sqldata_zts} -p out_ttyh_sqldata_zc=${out_ttyh_sqldata_zc} -p out_ttyh_sqldata_za=${out_ttyh_sqldata_za} -p out_ttyh_sqldata_grp_zt=${out_ttyh_sqldata_grp_zt} -p out_ttyh_sqldata_grp_zs=${out_ttyh_sqldata_grp_zs} -p out_ttyh_sqldata_grp_zc=${out_ttyh_sqldata_grp_zc} -p out_ttyh_sqldata_grp_za=${out_ttyh_sqldata_grp_za} -p out_ttyh_sqldata_static_type=${out_ttyh_sqldata_static_type} -p out_ttyh_sqldata_static_shop=${out_ttyh_sqldata_static_shop} -p out_ttyh_sqldata_static_channel=${out_ttyh_sqldata_static_channel} -p out_ttyh_sqldata_static_area=${out_ttyh_sqldata_static_area} -p out_ttyh_sqldata_static_article=${out_ttyh_sqldata_static_article} ${local_pig_script}/ttyhCleanSql.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;
