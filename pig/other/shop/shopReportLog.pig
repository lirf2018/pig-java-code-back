--------------------------商家日志 -----------------------

allData_shop = load '$input_clean_shop_log' using PigStorage('`') As (logtype:chararray,userOnly:chararray,userId:chararray,channel:chararray,shopId:chararray,areaCode:chararray,createTime:chararray);
fch_allData_shop = foreach allData_shop generate userOnly,userId,shopId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),channel,areaCode;

--输出=用户标识(唯一)，用户ID,商家Id,日期,时间,渠道,地区
store fch_allData_shop into '$out_clean_shop_log' using PigStorage('`');






