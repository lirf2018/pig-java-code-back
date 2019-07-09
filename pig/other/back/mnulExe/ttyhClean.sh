#按实际要求恢复数据
#!/bin/bash
#source /etc/profile
##date
#date=$(date +"%Y-%m-%d" -d "$1");
#former=$(date +"%Y-%m-%d" -d "$1-1day");
#former2=$(date +"%y-%m" -d "$1");
#filePre=$(date +"%d" -d "$1-1day");

##(手动执行的时候打开并输入对应的日期)------开始
##filePre=$1
##former=2016-12-${filePre}
##former2=16-12

#!/bin/bash
if [ ! -n "$1" ] ;then
    echo "请输入清洗日志时间,格式如:2016-11-11"
    exit
else
    echo "清洗日志时间: $1"
fi


year_month=$(date +"%y-%m" -d "$1")
d=$(date +"%d" -d "$1")
out_file_yearmonthdate=$(date +"%Y-%m-%d" -d "$1")

former2=$year_month
filePre=$d
former=$out_file_yearmonthdate
#echo $year_month
#echo $date
#echo $out_file_yearmonthdate

##(手动执行的时候打开并输入对应的日期)------结束



#代码路径
local_pig_script=/home/hadoop/lrfdata/pig/mnulExe

###原始日志路径
input_log_path=/user/datum/uhuibao/tmp/data/ttyh/${former2}/ttyh_${filePre}*

#判断分析的日志否存在
hadoop fs -test -e ${input_log_path}
if [ $? -eq 0 ]
	then
	echo ""
else
	echo "清洗日志不存在"${input_log_path}
        exit
fi



###日志输出
#资讯日志输出路径
output_article=/user/datum/uhuibao/clean/ttyh/${former}/article
#商家日志输出路径
output_shop=/user/datum/uhuibao/clean/ttyh/${former}/shop
#优惠券日志输出路径
output_ticket=/user/datum/uhuibao/clean/ttyh/${former}/ticket
#服务日志输出路径
output_service=/user/datum/uhuibao/clean/ttyh/${former}/service
#资讯收藏输出路径
output_collect=/user/datum/uhuibao/clean/ttyh/${former}/collect
#微信分享输出路径
output_share=/user/datum/uhuibao/clean/ttyh/${former}/share
#淘流量输出路径
output_taoflow=/user/datum/uhuibao/clean/ttyh/${former}/taoflow

#start

pig -p input_log_path=${input_log_path} -p output_article=${output_article} -p output_shop=${output_shop} -p output_ticket=${output_ticket} -p output_service=${output_service} -p output_collect=${output_collect} -p output_share=${output_share} -p output_taoflow=${output_taoflow} ${local_pig_script}/ttyhClean.pig

#for i in {11..20};do sh ttyhClean.sh 2017$i;done;
