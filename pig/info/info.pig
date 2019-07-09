REGISTER /home/hadoop/pig_exe/jars/fastjson-1.2.54.jar
REGISTER /home/hadoop/pig_exe/jars/commons-lang3-3.3.1.jar
REGISTER /home/hadoop/pig_exe/jars/pig-java-code.jar;
DEFINE InfoBusinessParam com.yufan.pig.info.InfoBusinessParam();
DEFINE InfoResponseTime com.yufan.pig.info.InfoResponseTime();

--input data
allData = load '$in_hdf_path_data' USING PigStorage('`') as line:chararray;
--filter business
businessData = filter allData by $0 matches '.*req_type.*';

--自定义函数处理数据
udfCleanBusinessData = foreach businessData generate flatten(InfoBusinessParam(line)) As (dateStr:chararray,reqType:chararray);

--not null
fCleanBusinessData = filter udfCleanBusinessData by reqType is not null;

--统计计算
groupBusiness = group fCleanBusinessData by (dateStr,reqType);
foreachGroupBusiness = foreach groupBusiness generate flatten(group) as (dateStr:chararray,reqType:chararray),COUNT(fCleanBusinessData.reqType) as c:long;

--保存清洗好的数据dateStr(日期),reqType(业务类型),c(访问次数)
store foreachGroupBusiness into '$out_clean_hdf_path1' USING PigStorage ('`');




--过滤数据(处理业务类响应时间)
businessTimeData = filter allData by $0 matches '.*InfoAction.*' and $0 matches '.*\u6267\u884c\u8017\u65f6.*' and $0 matches '.*type=.*';

--自定义函数处理数据
udfCleanBusinessTimeData = foreach businessTimeData generate flatten(InfoResponseTime(line)) As (dateStr:chararray,reqType:chararray,useTime:long);

--not null
fCleanBusinessTimeData = filter udfCleanBusinessTimeData by useTime is not null and reqType is not null;

--统计
groupBusinessTime = group fCleanBusinessTimeData by (dateStr,reqType);
foreachGroupBusinessAllTime = foreach groupBusinessTime generate flatten(group) as (dateStr:chararray,reqType:chararray),SUM(fCleanBusinessTimeData.useTime) as useTimeAll:long,COUNT(fCleanBusinessTimeData.reqType) as ct:long;

--保存清洗好的数据 dateStr(日期),reqType(业务类型),useTimeAll(总用时),ct(访问次数)
store foreachGroupBusinessAllTime into '$out_clean_hdf_path2' USING PigStorage ('`');


--保存最后结果
--foreachGroupBusiness=dateStr,reqType,c
--foreachGroupBusinessAllTime=dateStr,reqType,useTimeAll,ct
joinResultBusiness = join foreachGroupBusiness by ($0,$1) left outer,foreachGroupBusinessAllTime by ($0,$1);
resultBusiness = foreach joinResultBusiness generate $0,$1,$2,$5;
--输出:dateStr,reqType,c,useTimeAll
store resultBusiness into '$out_result_hdf_path' USING PigStorage ('`');






