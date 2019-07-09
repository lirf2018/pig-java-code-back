--------------------------服务日志 -----------------------

allData_service = load '$input_clean_service_log' using PigStorage('`') As (logtype:chararray,userOnly:chararray,userId:chararray,channel:chararray,serviceId:chararray,ifbuy:chararray,buyCount:chararray,shopId:chararray,createTime:chararray);
fch_allData_service = foreach allData_service generate userOnly,userId,serviceId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),channel,ifbuy,buyCount,shopId;

--输出=用户标识(唯一)，用户ID,服务Id,日期,时间,渠道,是否统计购买,购买数,商家id
store fch_allData_service into '$out_clean_service_log' using PigStorage('`');

















