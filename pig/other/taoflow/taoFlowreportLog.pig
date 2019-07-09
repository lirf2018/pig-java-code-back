--REGISTER /home/datum/uhuibao/jars/DealWithArticelSqlGroup.jar;
REGISTER /home/hadoop/lrfdata/jars/DealWithArticelSqlGroup.jar;
DEFINE DealWithArticelSqlGroup com.uhuibao.pigreljar.DealWithArticelSqlGroup();

------------------------对淘流量日志处理---------------
--- --------对任务中间数据处理
--任务类型
grp_allData_tasktype = load '$input_grp_tasktype_path' using PigStorage('`') As (allData_taskt:chararray);
fch_allData_tasktype = foreach grp_allData_tasktype generate flatten(DealWithArticelSqlGroup(allData_taskt)) As (taskId:chararray,typeIds:chararray);
--任务属性
grp_allData_taskproperty = load '$input_grp_taskproperty_path' using PigStorage('`') As (allData_taskp:chararray);
fch_allData_taskproperty = foreach grp_allData_taskproperty generate flatten(DealWithArticelSqlGroup(allData_taskp)) As (taskId:chararray,propertys:chararray);
--任务类别
grp_allData_taskcategory = load '$input_grp_taskcategory_path' using PigStorage('`') As (allData_taskc:chararray);
fch_allData_taskcategory = foreach grp_allData_taskcategory generate flatten(DealWithArticelSqlGroup(allData_taskc)) As (taskId:chararray,categorys:chararray);

----------------------任务静态表(为了得到任务总份数,开始时间结束时间) (任务id`任务名称`总份数`开始时间`结束时间`)
static_task_info = load '$input_static_task_path' using PigStorage('`') As (taskId:chararray,taskName:chararray,taskCount:long,startTime:chararray,endTime:chararray);



--------------第一次清洗的日志(日志类型（taoflow）;用户标识(唯一);用户ID;渠道编码标识;淘流量任务ID;（区分统计完成数,1统计完成,0统计未完成）;商家id;送流量币;创建时间)
allData_taoflow_log = load '$input_taoflw_log_path' using PigStorage('`') As(logType:chararray,userOnly:chararray,userId:chararray,channelCode:chararray,taskId:chararray,remarkType:chararray,shopId:chararray,getC:chararray,createTime:chararray);
fch_allData_taoflow_log = foreach allData_taoflow_log generate taskId,userOnly,userId,channelCode,remarkType,shopId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),getC;

--连接类型
--fch_allData_taoflow_log=taskId+userOnly+userId+channelCode+remarkType+shopId+dtime+mins+getC
--fch_allData_tasktype=taskId+types
join_data1 = join fch_allData_taoflow_log by $0 left outer,fch_allData_tasktype by $0;
--连接属性
--join_data1=taskId+userOnly+userId+channelCode+remarkType+shopId+dtime+mins+getC+taskId+types
--fch_allData_taskproperty=taskId+propertys
join_data2 = join join_data1 by $0 left outer,fch_allData_taskproperty by $0;
--连接类别
--join_data2=taskId+userOnly+userId+channelCode+remarkType+shopId+dtime+mins+getC+taskId+types+taskId+propertys
--fch_allData_taskcategory=taskId+categorys
join_data3 = join join_data2 by $0 left outer,fch_allData_taskcategory by $0;
-- 连接任务信息
--join_data3=taskId+userOnly+userId+channelCode+remarkType+shopId+dtime+mins+getC+taskId+types+taskId+propertys+taskId+categorys
--static_task_info=taskId+taskName+taskCount+startTime+endTime
join_data4 = join join_data3 by $0 left outer,static_task_info by $0;

--join_data4=taskId+userOnly+userId+channelCode+remarkType+shopId+dtime+mins+getC+taskId+types+taskId+propertys+taskId+categorys+taskId+taskName+taskCount+startTime+endTime
result1= foreach join_data4 generate $0,$1,$2,(($3 is null)?'uhb':$3),$4,$5,$6,$7,$8,$10,$12,$14,(($17 is null)?0L:$17),$18,$19;
--输出=taskId+userOnly+userId+channelCode+remarkType+shopId+dtime+mins+getC+types+propertys+categorys+taskCount+startTime+endTime
store result1 into '$out_clean2_taoflow_log' using PigStorage('`');


--------------清洗保存任务分享
--2017-03-03至 当前 的分享日志 （分享日志增加地区） 前的脚本
allData_share = load '$input_clean_share_log' using PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,shareFrom:chararray,url:chararray,shareTitle:chararray,areaCode:chararray,createTime:chararray);
fi_allData_share = filter allData_share by url matches '.*task/.*' and not url matches '.*news/.*';
fch_fi_allData_share = foreach fi_allData_share generate userIdOnly,userId,channelId,shareFrom,areaCode,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),flatten(STRSPLIT(url,'task/')) As(furl:chararray,taskId:chararray);
fch_fch_fi_allData_share = foreach fch_fi_allData_share generate userIdOnly,userId,channelId,shareFrom,areaCode,dtime,mins,flatten(STRSPLIT(taskId,'\\u003F')) As(taskId:chararray,urlo:chararray);
fch_fch_fch_fi_allData_share = foreach fch_fch_fi_allData_share generate taskId,userIdOnly,userId,dtime,mins,shareFrom,channelId,areaCode;

--连接类型
--fch_fch_fch_fi_allData_share=taskId+userIdOnly+userId+dtime+mins+shareFrom+channelId+areaCode
--fch_allData_tasktype=taskId+types
join_data1_ = join fch_fch_fch_fi_allData_share by $0 left outer,fch_allData_tasktype by $0;
--连接属性
--join_data1_=taskId+userIdOnly+userId+dtime+mins+shareFrom+channelId+areaCode+taskId+types
--fch_allData_taskproperty=taskId+propertys
join_data2_ = join join_data1_ by $0 left outer,fch_allData_taskproperty by $0;
--连接类别
--join_data2_=taskId+userIdOnly+userId+dtime+mins+shareFrom+channelId+areaCode+taskId+types+taskId+propertys
--fch_allData_taskcategory=taskId+categorys
join_data3_ = join join_data2_ by $0 left outer,fch_allData_taskcategory by $0;
-- 连接任务信息
--join_data3_=taskId+userOnly+userId+channelCode+remarkType+shopId+dtime+mins+getC+taskId+types+taskId+propertys+taskId+categorys
--static_task_info=taskId+taskName+taskCount+startTime+endTime
join_data4_ = join join_data3_ by $0 left outer,static_task_info by $0;

--join_data4=taskId+userIdOnly+userId+dtime+mins+shareFrom+channelId+areaCode+taskId+types+taskId+propertys+taskId+categorys+taskId+taskName+taskCount+startTime+endTime
result1_ = foreach join_data4_ generate $0,$1,$2,$3,$4,$5,(($6 is null)?'uhb':$6),$7,$9,$11,$13,(($16 is null)?0L:$16),$17,$18;
--输出=taskId+userIdOnly+userId+dtime+mins+shareFrom+channelId+areaCode+types+propertys+categorys+taskCount+startTime+endTime
store result1_ into '$out_clean2_taoflowshare_log' using PigStorage('`');


















