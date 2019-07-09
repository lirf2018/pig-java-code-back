REGISTER /home/datum/uhuibao/jars/DealWithArticelSqlGroup.jar;
--REGISTER /home/hadoop/lrfdata/jars/DealWithArticelSqlGroup.jar;
DEFINE DealWithArticelSqlGroup com.uhuibao.pigreljar.DealWithArticelSqlGroup();
-- 商家卡片月报日志
--先处理sql分组的中间数据(商家卡片对应各类的分组数据)
--得到商家卡片与对应的渠道字符串
data_grp_scc = load '$input_channel_path' using PigStorage('`') As (alldataSCC:chararray);
dw_data_grp_scc = foreach data_grp_scc generate flatten(DealWithArticelSqlGroup(alldataSCC)) As (shopId:chararray,channelCode:chararray);
--得到商家卡片与对应的地区字符串
data_grp_scs = load '$input_area_path' using PigStorage('`') As (alldataSCS:chararray);
dw_data_grp_scs = foreach data_grp_scs generate flatten(DealWithArticelSqlGroup(alldataSCS)) As (shopId:chararray,areaCode:chararray);

------计算商家卡片pv和uv
alldata_clean_shopcard = load '$input_shopcard_log_path' using PigStorage('`') As (logtype:chararray,userOnly:chararray,userId:chararray,channelCode:chararray,shopId:chararray,areaCode:chararray,createTime:chararray);
sp_alldata_clean_shopcard_pv = foreach alldata_clean_shopcard generate shopId,userOnly,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray);
--计算商家卡片pv
fch_sp_alldata_clean_shopcard_pv = foreach sp_alldata_clean_shopcard_pv generate shopId,dtime,userOnly;
grp_fch_sp_alldata_clean_shopcard_pv = group fch_sp_alldata_clean_shopcard_pv by (shopId,dtime);
fch_grp_fch_sp_alldata_clean_shopcard_pv = foreach grp_fch_sp_alldata_clean_shopcard_pv generate flatten(group) As (shopId:chararray,dtime:chararray),COUNT(fch_sp_alldata_clean_shopcard_pv) As pv:long;
--计算商家卡片uv
dis_fch_sp_alldata_clean_shopcard_uv = distinct fch_sp_alldata_clean_shopcard_pv;
grp_dis_fch_sp_alldata_clean_shopcard_uv = group dis_fch_sp_alldata_clean_shopcard_uv by (shopId,dtime);
fch_grp_dis_fch_sp_alldata_clean_shopcard_uv = foreach grp_dis_fch_sp_alldata_clean_shopcard_uv generate flatten(group) As (shopId:chararray,dtime:chararray),COUNT(dis_fch_sp_alldata_clean_shopcard_uv) As uv:long;
-- 计算商家卡片数
allData_shopcard = load '$input_shopcard_path' using PigStorage('`') As (shopId:chararray,shopcardName);
grp_allData_shopcard = group allData_shopcard all; 
fch_grp_allData_shopcard = foreach grp_allData_shopcard generate COUNT(allData_shopcard) As scCount:long,'$static_log_time' As dtime:chararray;--从外部传入计算时间,以方便连接

--连接结果 pv,uv
--fch_grp_fch_sp_alldata_clean_shopcard_pv=shopId+time+pv
--fch_grp_dis_fch_sp_alldata_clean_shopcard_uv=shopId+time+uv
join_data1 = join fch_grp_fch_sp_alldata_clean_shopcard_pv by $0 left outer,fch_grp_dis_fch_sp_alldata_clean_shopcard_uv by $0;
--连接渠道
--join_data1=shopId+time+pv+shopId+time+uv
--dw_data_grp_scc=shopId+channelCode
join_data2 = join join_data1 by $0 left outer,dw_data_grp_scc by $0;
--连接地区
--join_data2=shopId+time+pv+shopId+time+uv+shopId+channelCode
--dw_data_grp_scs = shopId+areaCode
join_data3 = join join_data2 by $0 left outer,dw_data_grp_scs by $0;
--连接名称
--join_data3=shopId+time+pv+shopId+time+uv+shopId+channelCode+shopId+areaCode
--allData_shopcard=shopId+shopcardName
join_data4 = join join_data3 by $0 left outer,allData_shopcard by $0;
-- 连接卡片数
--join_data4=shopId+time+pv+shopId+time+uv+shopId+channelCode+shopId+areaCode+shopId+shopcardName
--fch_grp_allData_shopcard=scCount+dtime;
join_data5 = join join_data4 by $1 left outer,fch_grp_allData_shopcard by $1;
--
--join_data5=shopId+time+pv+shopId+time+uv+shopId+channelCode+shopId+areaCode+shopId+shopcardName+scCount+dtime
--结果:商家卡片id+商家卡片名称，pv,uv，卡片数，渠道，地区，日志时间
result = foreach join_data5 generate $0,$11,(($2 is null)?0L:$2),(($5 is null)?0L:$5),(($12 is null)?0L:$12),$7,$9,$1;
store result into '$out_result_path' using PigStorage('`');
















