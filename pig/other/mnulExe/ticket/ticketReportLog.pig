REGISTER /home/datum/uhuibao/jars/DealWithArticelSqlGroup.jar;
--REGISTER /home/hadoop/lrfdata/jars/DealWithArticelSqlGroup.jar;
DEFINE DealWithArticelSqlGroup com.uhuibao.pigreljar.DealWithArticelSqlGroup();
----------------------处理卡券类日志----------------------------
--处理中间数据（卡券类型分组）---------得到卡券类别
data_ticket_type_grp = load '$input_grp_ticket_types_path' using PigStorage('`') As (alldataTT:chararray);
fch_data_ticket_type_grp = foreach data_ticket_type_grp generate flatten(DealWithArticelSqlGroup(alldataTT)) As (ticketId:chararray,typeIds:chararray);


--------加载卡券库存等 信息
--加载卡券关联数据（卡券Id`卡券名称`库存`开始时间`结束时间`类型id`类型名称）
data_ticket_type = load '$input_static_ticket_data' using PigStorage('`') As (ticketId,ticketName,kc,startTime,endTime,typeId,typeName);
fch_data_ticket_type = foreach data_ticket_type generate ticketId,kc,startTime,endTime;
ticket_info = distinct fch_data_ticket_type;

----------加载卡券清洗后日志
allData_ticket = load '$input_ticket_clean_log_path' USING PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channel:chararray,ticketId:chararray,ticketOP:int,getCount:long,shopId:chararray,createTime:chararray);
fch_allData_ticket = foreach allData_ticket generate ticketId,userIdOnly,userId,ticketOP,getCount,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),channel,shopId;

--连接卡券和卡券信息
--fch_allData_ticket=ticketId+userIdOnly+userId+ticketOP+getCount+dtime+mins+channel+shopId;
--ticket_info=ticketId+kc+startTime+endTime
join_data1 = join fch_allData_ticket by $0 left outer,ticket_info by $0;
--连接卡券类别
--join_data1=ticketId+userIdOnly+userId+ticketOP+getCount+dtime+mins+channel+shopId+ticketId+kc+startTime+endTime
--fch_data_ticket_type=ticketId+typeIds
join_data2 = join join_data1 by $0 left outer,fch_data_ticket_type_grp by $0;

--join_data2=ticketId+userIdOnly+userId+ticketOP+getCount+dtime+mins+channel+shopId+ticketId+kc+startTime+endTime+ticketId+typeIds
--结果=卡券id,用户Id唯一,用户id,操作类型,领取数量,日期,时间,渠道,店铺,库存,开始时间,结束时间,类别
result = foreach join_data2 generate $0,$1,$2,$3,$4,$5,$6,$7,$8,$10,$11,$12,$14;
store result into '$out_ticket_clean_log_path' using PigStorage('`');

