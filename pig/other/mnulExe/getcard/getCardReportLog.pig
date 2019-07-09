------------领卡日志

allData = load '$input_getcard_log' as strs:chararray; 
fl_allData = filter allData by $0 matches '.*/sim/take.*';

sp_fl_allData = foreach fl_allData generate flatten(STRSPLIT(strs,'`')) As(logURL:chararray,channelCode:chararray,areaCode:chararray,userOnly:chararray,userId:chararray,deviceType:chararray,createTime:chararray);

fch_sp_fl_allData = foreach sp_fl_allData generate channelCode,areaCode,userOnly,userId,deviceType,flatten(STRSPLIT(createTime,' ')) as (dtime:chararray,time:chararray);

----保存领卡日志

--结果=渠道+地区+用户标识+用户ID+设备信息+日期+时间
store fch_sp_fl_allData into '$out_clean_getcard_log' using PigStorage('`');


--------------------------------------------------------处理订单系统数据
-----------------------------处理领卡的订单
/**
0---日志类型|1---订单ID|2---用户Id|3---订单号|4---订单类型|5---订购产品数|6---订单总价|7---收货人|8---收货人手机|9---收货地址|10---支付方式|11---支付交易号|12---渠道|
13---邮费|14---邮寄方式（1快递2平邮3EMS4自取）|15---邮寄公司编码|16---邮寄号|17---订单备注|18---订单状态（）|19---下单时间|20---支付时间|21---邮寄时间|22---业务类型|
23---优惠金额数|24---优惠说明|25---修改人|26---修改时间|27---支付系统中支付的价格|28---收款方|29---产品类型|30---订单实际价格|31---下单来源|32---对账单号|
33---订单类别|34---退款原因|35---余款总金额|36---币种类别 
*/
orderdb = load '$input_ordersysdb_log' as allData:chararray;

-- 保留订单表数据
fi_orderdb = filter orderdb by allData matches 'order;.*';
order_data = foreach fi_orderdb generate flatten(STRSPLIT(allData,'`'));
allorder = foreach order_data generate $1 As orderId:chararray, $22 As business:chararray, $3 As orderNum:chararray, $2 As userId:chararray, $8 As phone:chararray, $9 As addr:chararray, $14 As getWay:chararray, $6 As orderPrice:chararray, $12 As channel:chararray, $19 As orderTime:chararray, $18 As orderStatus:chararray;
-- 保留订单：1用户ID不为0;2订单状态为已付款--1、已发货--2，已完成的订单---3;3订单为领卡订单--10
fi_allorder_getcard = filter allorder by (userId!='0' and business=='10' and (orderStatus=='1' or orderStatus=='2' or orderStatus=='3'));

-- 订单系统参数-----获取运送方式
-- 0---日志类型|1---参数标识|2---参数类型|3---参数名称|4---参数编码|5---参数值|6---备注
paramdata = filter orderdb by (allData matches 'param_code;.*' and allData matches '.*CARRY_WAY.*');
sp_paramdata = foreach paramdata generate flatten(STRSPLIT(allData,'`'));
paramdata_getway = foreach sp_paramdata generate $3 As getWay:chararray, $4 As getWayName:chararray;

-- 链接运送方式
data1 = join fi_allorder_getcard by getWay left outer,paramdata_getway by getWay;




-----处理卡类型(卡备注名称)
-- 本次订单详情
/**
-- 0---日志类型|1---详情ID|2---订单ID|3---产品标识|4---产品价格|5---产品名称|6---购买数量|7---产品规格|8---领取地址标识|9---领取地址|10---领取时间|
11---详情状态|12---修改人|13---修改时间|14---详情备注|15---余款金额|16---余款支付金额|17---支付系统中支付的价格
*/
today_order = foreach fi_allorder_getcard generate orderId,getWay;

fi_orderdeatil = filter orderdb by allData matches 'order_detail;.*';
orderdetail_data = foreach fi_orderdeatil generate flatten(STRSPLIT(allData,'`')); 
-- 得到当前的订单详情
join_order = join today_order by $0,orderdetail_data by $2;


-- 带充值的订单
rech_orderdetail_data = filter join_order by $5=='0' ;
fch_rech_orderdetail_data = foreach rech_orderdetail_data generate $0 As orderId,$7 As preductNume;

--处理卡类型（商品备注名称）
/**
由于增加连接后增加了订单号,运送方式，所以订单详情美国项往后移2位
join_order= 第一个位，订单id,后面依次加2位
0---日志类型|1---详情ID|2---订单ID|3---产品标识|4---产品价格|5---产品名称|6---购买数量|7---产品规格|8---领取地址标识|9---领取地址|10---领取时间|
11---详情状态|12---修改人|13---修改时间|14---详情备注|15---余款金额|16---余款支付金额|17---支付系统中支付的价格
*/
--详情属性
-- 0---日志类型|1---属性标识|2---详情标识|3---键|4---值|5---修改人|6---修改时间
orderdetailproperty = filter orderdb by (allData matches 'detail_property;.*' and allData matches '.*iccid_name.*');
sp_orderdetailproperty = foreach orderdetailproperty generate flatten(STRSPLIT(allData,'`')); 
join_detailproperty = join join_order by $3,sp_orderdetailproperty by  $2;
-- 关联卡类型
-- data1=订单id+备注名称
data2 = foreach join_detailproperty generate $0 As orderId:chararray,$23 As cardName:chararray;
dis_data2 = distinct data2;
-- 得到订单和卡备注名称
data3 = join data1 by $0 left outer,dis_data2 by $0;
--- 关联是否带充值
data4 = join data3 by $0 left outer,fch_rech_orderdetail_data by orderId;
--data4=orderId+business+orderNum+userId+phone+addr+getWay+orderPrice+channel+orderTime+orderStatus+getWay+getWayName+orderId+cardName+orderId+rechangeName;

--结果=orderNum+userId+phone+addr+orderPrice+channel+orderTime+orderStatus+getWayName+cardName+if_rechage
result1 = foreach data4 generate $2,$3,$4,$5,$7,$8,$9,$10,(($12 is null)?$6:$12),$14,(($16 is null)?'0':'1');
result1_ = distinct result1;
store result1_ into '$out_order_path' using PigStorage('`');

----------------------------处理领卡已退款或者已取消的订单
-- 加载当前处理的已取消或已退款的订单
cancelorderdata = filter orderdb by allData matches 'order_inday;.*';
sp_cancelorder = foreach cancelorderdata generate flatten(STRSPLIT(allData,'`'));
cancelorder = foreach sp_cancelorder generate $1 As orderId:chararray,$2 As userId:chararray, $3 As orderNum:chararray, $18 As orderStatus:chararray, $22 As business:chararray;
fi_cancelorder = filter cancelorder by (userId!='0' and business=='10' and (orderStatus=='4' or orderStatus=='6'));

result2 = foreach fi_cancelorder generate orderNum,orderStatus;
result2_ = distinct result2;
store result2_ into '$out_cancelorder_path' using PigStorage('`');





