--测试项目数据web日志数据清洗 (按天统计)
REGISTER /home/hadoop/pig_exe/jars/CleanWeb.jar;

DEFINE cleanWeb com.uhuibao.pigreljar.CleanWeb();
/*
清洗阶段
*/
-- 加载web日志
--webData = load 'hdfs://h1:9000/web/data/*' As (allStr:chararray);

--cleanData = foreach webData generate FLATTEN(cleanWeb(allStr));

-- 清洗后的数据
-- store cleanData into 'hdfs://h1:9000/web/clean/';

-- 1浏览量pv
-- alldata = foreach cleanData generate STRSPLIT('`') As (ip:chararray,time:chararray,url:chararray);

/*
计算阶段
*/
-- 1浏览量pv
webData = LOAD 'hdfs://h1:9000/web/clean/*' USING PigStorage('`') As (ip:chararray,time:chararray,url:chararray);
--store webData into 'hdfs://h1:9000/web/retult/ipd1_113/';
grppv = group webData by time;
grp_pv_d = FOREACH grppv GENERATE FLATTEN(group) As time:chararray , COUNT(webData.time) As pvc:long;
--grp_pv_d1 = FOREACH grppv GENERATE FLATTEN(group) As time:chararray , COUNT(webData) As pvc:long;
--store grp_pv_d into 'hdfs://h1:9000/web/retult/pvd/';
--store grp_pv_d1 into 'hdfs://h1:9000/web/retult/pvd1/';
-- 2注册用户数
--(180.141.173.114	2013-05-31	/member.php?mod=register&inajax=1)
--(69.197.189.38	2013-05-31	/member.php?mod=register)
webDatauv = FILTER webData BY url matches '.*member.php\\u003Fmod=register.*' and  not url matches '.*inajax=1.*'; 
--store webDatauv into 'hdfs://h1:9000/web/retult/rguvd3/';
grpuv = group webDatauv by time;
grpuv_d = FOREACH grpuv GENERATE FLATTEN(group) As time:chararray , COUNT(webDatauv.time)  As uvc:long;
--grpuv_d = FOREACH grpuv GENERATE FLATTEN(group) As time:chararray , COUNT(webDatauv)  As uvc:long;
--store grpuv_d into 'hdfs://h1:9000/web/retult/rguvd/';
--store grpuv_d1 into 'hdfs://h1:9000/web/retult/rguvd1/';
-- 3ip数
needipwebdatas = FOREACH webData GENERATE ip,time;
--store needipwebdatas into 'hdfs://h1:9000/web/retult/qc1/';
needipwebdata = DISTINCT needipwebdatas; -- 去重
--store needipwebdata into 'hdfs://h1:9000/web/retult/qc/';
grpip = group needipwebdata by (time,ip);
--store grpip into 'hdfs://h1:9000/web/retult/ipd1_112/';
grp1ip_d = FOREACH grpip GENERATE FLATTEN(group) As(ip:chararray,time:chararray) , COUNT(needipwebdata.ip) As ipc:long;
--grp1ip_d1 = FOREACH grpip GENERATE FLATTEN(group) As(time:chararray,ip:chararray) , COUNT(needipwebdata) As ipc:long;
--store  grp1ip_d  into 'hdfs://h1:9000/web/retult/ipc/';
--store  grp1ip_d1  into 'hdfs://h1:9000/web/retult/ipd1/';
-- 4跳出率   当天只访问一次的Ip/pv
grptc = group needipwebdatas by (ip,time);
--store grptc into 'hdfs://h1:9000/web/retult/ipd1_111/';
grptc_d = FOREACH grptc GENERATE FLATTEN(group) As (ip:chararray,time:chararray) , COUNT(needipwebdatas.ip) As ipct:long;
--grptc_d1 = FOREACH grptc GENERATE FLATTEN(group) As (time:chararray,ip:chararray) , COUNT(needipwebdatas) As ipct:long;
--store  grptc_d  into 'hdfs://h1:9000/web/retult/tcd/';
--store  grptc_d1  into 'hdfs://h1:9000/web/retult/tcd1/';

--grptc_d = FOREACH grptc GENERATE group , COUNT(needipwebdatas);
--store  grptc_d  into 'hdfs://h1:9000/web/retult/tcd2/';

grptc_d_f = FILTER grptc_d BY ipct==1;
grptc_d_g = group grptc_d_f by time;
--store grptc_d_g into 'hdfs://h1:9000/web/retult/tcd2_/';
grptc_d_e = FOREACH grptc_d_g GENERATE FLATTEN(group) As time:chararray,COUNT(grptc_d_f) As ipctd:long;
--store grptc_d_e into 'hdfs://h1:9000/web/retult/tcd2/';
joind = JOIN grp_pv_d by time,grptc_d_e by time;
--store joind into 'hdfs://h1:9000/web/retult/tcd3/';
--result
tcld = foreach joind generate $0,(float)$3/$1 As tcldresult:float;
--store tcld into 'hdfs://h1:9000/web/retult/tcd4/';

-- last result
last_result_ = JOIN grp_pv_d BY time,grpuv_d By time;
store last_result_ into 'hdfs://h1:9000/web/retult/result_';
--last_result_1 = JOIN grp1ip_d BY time,tcld BY $0;
--store last_result_1 into 'hdfs://h1:9000/web/retult/result_1';

--last_result_2 = JOIN last_result_ BY $0,last_result_1 BY $0;
--store last_result_2 into 'hdfs://h1:9000/web/retult/result_';
--last_result = FOREACH last_result_2 GENERATE $0,$1,$3,$5,$7;
--store last_result into 'hdfs://h1:9000/web/retult/result';
-- 5板块热度排行

