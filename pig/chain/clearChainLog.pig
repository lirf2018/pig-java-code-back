REGISTER /home/hadoop/pig_exe/jars/fastjson-1.2.54.jar
REGISTER /home/hadoop/pig_exe/jars/pig-java-code.jar;
DEFINE CleanChainLog com.yufan.chain.CleanChainLog();

--加载数据
allData = load '$in_hdf_path_data' USING PigStorage('`') as all:chararray;
--过滤数据
fData = filter allData by $0 matches '.*req_type.*';

--自定义函数处理数据
udfCleanData = foreach fData generate flatten(CleanChainLog($0)) As (reqType:chararray,dateStr:chararray);

--去空
fCleanData = filter udfCleanData by reqType is not null;

--保存清洗好的数据
store fCleanData into '$out_clean_hdf_path' USING PigStorage ('`');

--统计
grouData = group fCleanData by (reqType,dateStr);
fGrouData = foreach grouData generate flatten(group) as (reqType:chararray,dateStr:chararray),COUNT(fCleanData.reqType) as c:long;
-- 保存统计数据
oData = order fGrouData by dateStr,c desc;
store oData into '$out_st_hdf_path' USING PigStorage ('`');
