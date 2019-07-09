-- 对h5日志处理
alldata = load '$input_log_path' USING PigStorage(';');

-- 清洗资讯日志
/**日志类型（article）;用户标识(唯一);用户ID;渠道编码标识;资讯ID;资讯地区编码(all所有地区);商家id;创建时间
   article;1s68udk108jc9a3rifsdq61cs0;0;uhb;2867;852;2016-10-23 00:01:46
*/
article = filter alldata by $0=='article';
store article into '$output_article' using PigStorage('`');

-- 商家日志描述
/**日志类型（shop）;用户标识(唯一);用户ID;渠道编码标识;商家ID;商家地区编码(all所有地区);创建时间
   shop;ldbpkp8shue64gfu8ict58rko3;0;uhuibao123;11012;852;2016-10-23 00:52:34
*/
shop = filter alldata by $0=='shop';
store shop into '$output_shop' using PigStorage('`');

-- 优惠券日志
/**日志类型（coupon）;用户标识(唯一);用户ID;渠道编码标识;优惠券ID;（区分统计领取数的,1统计领取,0非领取）;领取优惠券数量;商家id;创建时间
    coupon;hij2hgid4nf4gtso4biniopa42;0;uhb;72;0;0;2016-10-23 00:19:51
*/
coupon = filter alldata by $0=='coupon';
store coupon into '$output_ticket' using PigStorage('`');

-- 服务日志
/**日志类型（service）;用户标识(唯一);用户ID;渠道编码标识;服务id;（是否统计购买 0：浏览数，1：购买数）;购买数;商家ID;创建时间
   service;unm1otiuvqhd0vb2llo2tg6mf1;0;uhuibao123;12;0;0;138;2016-10-31 22:50:06
*/
service = filter alldata by $0=='service';
store service into '$output_service' using PigStorage('`');

-- 资讯收藏日志
/**日志类型（collect）;用户标识(唯一);用户ID;渠道编码标识;收藏途径(1-H5收藏);收藏URL地址;资讯ID;创建时间
   collect;t50tfasptskvf08bancnmam1m3;1044328;uhb;1;http://h5.uhuibao.com/v2/#/news/2810;2810;2016-11-01 01:02:43
*/
collect = filter alldata by $0=='collect';
store collect into '$output_collect' using PigStorage('`');

-- 微信分享日志
/**日志类型（share）;用户标识(唯一);用户ID;渠道编码标识;分享途径(1微信浏览器);分享页面URL;分享标题;创建时间
   share;3j6k9m7tl173os0tg78se5l1o4;1142918;uhb;1;http://h5.uhuibao.com/#!taoflow/task?task_id=148&key=c7783d851b05a7a8;游惠宝-阅读文章 |万圣节攻略早知道;2016-11-01 08:09:46
*/
share = filter alldata by $0=='share';
store share into '$output_share' using PigStorage('`');

-- 淘流量日志
/**日志类型（taoflow）;用户标识(唯一);用户ID;渠道编码标识;淘流量任务ID;（区分统计完成数,1统计完成,0统计未完成）;商家id;创建时间
   taoflow;bs8r6e17cqsn4i0iqg75jkdtp7;1003903;uhuibao123;1;0;2016-10-23 00:01:38
*/
taoflow = filter alldata by $0=='taoflow';
store taoflow into '$output_taoflow' using PigStorage('`');

