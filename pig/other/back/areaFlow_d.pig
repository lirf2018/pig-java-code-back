REGISTER /home/hadoop/pig_exe/jars/DealWithMcc.jar
DEFINE DealWithMcc com.uhuibao.pigreljar.DealWithMcc();
-- hdfs://h1:9000/uhuibao/clean/dcc/log18_2016-12-20/*
--


-- 按天统计地区渠道人次使用流量
--加载dcc数据
dccdata18 = load  'hdfs://h1:9000/uhuibao/test/log18_dcc_clean.txt' USING PigStorage('`') As (time:chararray,logClass:chararray,dccType:chararray,
	flag:chararray,hopByHop:chararray,endIden:chararray,seID:chararray,ccrType:chararray,ccrCount:chararray,imsi:chararray,cardID:chararray,flow:long,mcc:chararray,timeStamp:chararray,
	begin0100:chararray,staMccmnc:chararray,lac:chararray,cid:chararray,stationID:chararray,sgsnIp:chararray,Imei:chararray,Qos:chararray,pdpAddress:chararray,ratType:chararray);
--保留需要字段
needdccdata18 = foreach dccdata18 generate staMccmnc,imsi,flow;

needdccdata18_notnull = filter needdccdata18 by (flow>0) and (staMccmnc !='null');

needdccdata18_notnull_  = foreach needdccdata18_notnull generate SUBSTRING(staMccmnc,0,3) As(mcc:chararray),imsi As(imsi:chararray),flow As(flow:long);

--对数据分组
group_dcc = group needdccdata18_notnull_ by (mcc,imsi);

-- 求流量和
group_dcc_c = foreach group_dcc GENERATE FLATTEN(group) As (mcc:chararray,imsi:chararray) , SUM(needdccdata18_notnull_.flow) As (flow:long);

--store group_dcc_c_ into  'hdfs://h1:9000/uhuibao/test/dcc_result'; 

-- 加载imsi
imsidata = load 'hdfs://h1:9000/uhuibao/test/imsiAgent.txt' USING PigStorage('`') As (iccid:chararray,imsi:chararray,cost:chararray,passwd:chararray,monthfee:chararray,expdate:chararray,
        allfee:chararray,calledfee:chararray,callsmsfee:chararray,pukcode:chararray,agentcode:chararray,price:chararray,productId:int);
-- 保留需要的字段
imsidataneed = foreach imsidata generate imsi As imsi:chararray,agentcode As agentcode:chararray;



joindata = JOIN group_dcc_c by imsi ,imsidataneed by imsi;


store joindata into 'hdfs://h1:9000/uhuibao/test/result';

