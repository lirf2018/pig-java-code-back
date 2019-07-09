--REGISTER /home/datum/uhuibao/jars/DealWithArticelSqlGroup.jar;
REGISTER /home/hadoop/lrfdata/jars/DealWithArticelSqlGroup.jar;
DEFINE DealWithArticelSqlGroup com.uhuibao.pigreljar.DealWithArticelSqlGroup();
-- 优惠券月报日志
-- 加载数据
alldata = load '$input_ticket_log_path' USING PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channel:chararray,ticketId:chararray,ticketOP:int,getCount:long,shopId:chararray,createTime:chararray);
fch_alldata = foreach alldata generate logType,userIdOnly,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),ticketId,ticketOP,getCount;
-- 计算卡券的pv
fch_fch_alldata = foreach fch_alldata generate ticketId,userIdOnly,logType,dtime;
grp_fch_fch_alldata = group fch_fch_alldata by (ticketId,dtime);
fch_grp_fch_fch_alldata = foreach grp_fch_fch_alldata generate flatten(group) As (ticketId:chararray,dtime:chararray),COUNT(fch_fch_alldata) As pv:long;
--store fch_grp_fch_fch_alldata into 'hdfs://h1:9000/lirftest/pv';
-- 计算卡券的uv
dis_fch_fch_alldata = distinct fch_fch_alldata;
grp_dis_fch_fch_alldata = group dis_fch_fch_alldata by (ticketId,dtime);
fch_grp_dis_fch_fch_alldata = foreach grp_dis_fch_fch_alldata generate flatten(group) As (ticketId:chararray,dtime:chararray),COUNT(dis_fch_fch_alldata) As uv:long;
--store fch_grp_dis_fch_fch_alldata into 'hdfs://h1:9000/lirftest/uv';
-- 计算领取数
fi_fch_alldata = filter fch_alldata by ticketOP==1;
fch_fi_fch_alldata = foreach fi_fch_alldata generate ticketId,dtime,getCount;
grp_fch_fi_fch_alldata = group fch_fi_fch_alldata by (ticketId,dtime);
fch_grp_fch_fi_fch_alldata = foreach grp_fch_fi_fch_alldata generate flatten(group) As (ticketId:chararray,dtime:chararray),SUM(fch_fi_fch_alldata.getCount) As gCount:long;
--store fch_grp_fch_fi_fch_alldata into 'hdfs://h1:9000/lirftest/gc';
-- 计算卡券的核销数
fi_fch_alldata_ec = filter fch_alldata by ticketOP==2;
fch_fi_fch_alldata_ec = foreach fi_fch_alldata_ec generate ticketId,dtime;
grp_fch_fi_fch_alldata_ec = group fch_fi_fch_alldata_ec by (ticketId,dtime);
fch_grp_fch_fi_fch_alldata_ec = foreach grp_fch_fi_fch_alldata_ec generate flatten(group) As (ticketId:chararray,dtime:chararray),COUNT(fch_fi_fch_alldata_ec) As ecCount:long;
--store fch_grp_fch_fi_fch_alldata_ec into 'hdfs://h1:9000/lirftest/ec';
-- 从静态数据中得到卡券对应所有渠道
alldata_tc = load '$input_channel_path' using PigStorage('`') As (allParamTC:chararray);
fch_alldata_tc = foreach alldata_tc generate flatten(DealWithArticelSqlGroup(allParamTC)) As (ticketId:chararray,channelCodes:chararray);
--store fch_alldata_tc into 'hdfs://h1:9000/lirftest/tchannel';
-- 从静态数据中得到卡券对应所有商家
alldata_ts = load '$input_shop_path' using PigStorage('`') As (allParamTS:chararray);
fch_alldata_ts = foreach alldata_ts generate flatten(DealWithArticelSqlGroup(allParamTS)) As (ticketId:chararray,shopIds:chararray);
--store fch_alldata_ts into 'hdfs://h1:9000/lirftest/tshop';
-- 从静态数据中得到卡券所有类型
alldata_tt = load '$input_type_path' using PigStorage('`') As (allParamTT:chararray);
fch_alldata_tt = foreach alldata_tt generate flatten(DealWithArticelSqlGroup(allParamTT)) As (ticketId:chararray,typeIds:chararray);
--store fch_alldata_tt into 'hdfs://h1:9000/lirftest/ttype';
-- 从静态数据中得到卡券数
alldata_t = load '$input_ticket_path' using PigStorage('`') As (ticketId:chararray,ticketName:chararray,kc:int,startTime:chararray,endTime:chararray);
grp_alldata_t = group alldata_t all;
fch_grp_alldata_t = foreach grp_alldata_t generate COUNT(alldata_t) As tc:long,'$static_log_time' As dtime:chararray;--从外部传入计算时间,以方便连接
--store fch_grp_alldata_t into 'hdfs://h1:9000/lirftest/ticketc'; 

-- 连接pv+uv
--fch_grp_fch_fch_alldata=ticketId+dtime+pv
--fch_grp_dis_fch_fch_alldata=ticketId+dtime+uv
join_data1 = join fch_grp_fch_fch_alldata by $0 left outer,fch_grp_dis_fch_fch_alldata by $0;
--连接领取数
--join_data1=ticketId+dtime+pv+ticketId+dtime+uv
--fch_grp_fch_fi_fch_alldata=ticketId+dtime+gCount
join_data2 = join join_data1 by $0 left outer,fch_grp_fch_fi_fch_alldata by $0;
--连接核销数
--join_data2=ticketId+dtime+pv+ticketId+dtime+uv+ticketId+dtime+gCount
--fch_grp_fch_fi_fch_alldata_ec=ticketId+dtime+ecCount
join_data3 = join join_data2 by $0 left outer,fch_grp_fch_fi_fch_alldata_ec by $0;
--连接渠道
--join_data3=ticketId+dtime+pv+ticketId+dtime+uv+ticketId+dtime+gCount+ticketId+dtime+ecCount
--fch_alldata_tc=ticketId+channelCodes
join_data4 = join join_data3 by $0 left outer,fch_alldata_tc by $0;
--连接商家
--join_data4=ticketId+dtime+pv+ticketId+dtime+uv+ticketId+dtime+gCount+ticketId+dtime+ecCount+ticketId+channelCodes
--fch_alldata_ts=ticketId+shopIds
join_data5 = join join_data4 by $0 left outer,fch_alldata_ts by $0;
--连接类型
--join_data5=ticketId+dtime+pv+ticketId+dtime+uv+ticketId+dtime+gCount+ticketId+dtime+ecCount+ticketId+channelCodes+ticketId+shopIds
--fch_alldata_tt=ticketId+typeIds
join_data6 = join join_data5 by $0 left outer,fch_alldata_tt by $0;
--连接卡券信息
--join_data6=ticketId+dtime+pv+ticketId+dtime+uv+ticketId+dtime+gCount+ticketId+dtime+ecCount+ticketId+channelCodes+ticketId+shopIds+ticketId+typeIds
--alldata_tt=ticketId+ticketName+kc+startTime+endTime
join_data7 = join join_data6 by $0 left outer,alldata_t by $0;
-- 连接h5库中所有卡券数
join_data8 = join join_data7 by $1 left outer,fch_grp_alldata_t by $1;
--ticketId+dtime+pv+ticketId+dtime+uv+ticketId+dtime+gCount+ticketId+dtime+ecCount+ticketId+channelCodes+ticketId+shopIds+ticketId+typeIds+ticketId+ticketName+kc+startTime+endTime+tc+dtime
result = foreach join_data8 generate $0,$19,(($2 is null)?0L:$2),(($5 is null)?0L:$5),(($8 is null)?0L:$8),(($11 is null)?0L:$11),$13,$15,$17,(($20 is null)?0L:$20),(($21 is null)?'2000-01-01 00:00:00':$21),(($22 is null)?'2000-01-01 00:00:00':$22),$1,$23;
--结果卡券id+卡券名称+pv+uv+领取数+核销数+渠道s+商家s+类型s+库存+开始时间+结束时间+日志时间+h5卡券总数

store result into '$out_result_path' using PigStorage('`');

