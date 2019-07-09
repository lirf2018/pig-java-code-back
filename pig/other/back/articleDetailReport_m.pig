REGISTER /home/hadoop/lrfdata/jars/DealWithArticelSqlGroup.jar;
DEFINE DealWithArticelSqlGroup com.uhuibao.pigreljar.DealWithArticelSqlGroup();
--按资讯文章统计每个文章的pv,uv,collectCount,shareCount

--先处理sql分组的中间数据(资讯对应各类的分组数据)
--得到资讯与对应的类别字符串
data_grp_zt = load '$input_type_path' using PigStorage('`') As (alldataZT:chararray);
dw_data_grp_zt = foreach data_grp_zt generate flatten(DealWithArticelSqlGroup(alldataZT)) As (articleId:chararray,types:chararray);
--store dw_data_grp_zt into 'hdfs://h1:9000/lirftest/articlet_type' using PigStorage('`');
--得到资讯与对应的商家字符串
data_grp_zs = load '$input_shop_path' using PigStorage('`') As (alldataZS:chararray);
dw_data_grp_zs = foreach data_grp_zs generate flatten(DealWithArticelSqlGroup(alldataZS)) As (articleId:chararray,shops:chararray);
--store dw_data_grp_zs into 'hdfs://h1:9000/lirftest/articlet_shop' using PigStorage('`');
--得到资讯与对应的渠道字符串
data_grp_zc = load '$input_channel_path' using PigStorage('`') As (alldataZC:chararray);
dw_data_grp_zc = foreach data_grp_zc generate flatten(DealWithArticelSqlGroup(alldataZC)) As (articleId:chararray,channels:chararray);
--store dw_data_grp_zc into 'hdfs://h1:9000/lirftest/articlet_channel' using PigStorage('`');
--得到资讯与对应的地区字符串
data_grp_za = load '$input_area_path' using PigStorage('`') As (alldataZA:chararray);
dw_data_grp_za = foreach data_grp_za generate flatten(DealWithArticelSqlGroup(alldataZA)) As (articleId:chararray,areaCodes:chararray);
--store dw_data_grp_za into 'hdfs://h1:9000/lirftest/articlet_area' using PigStorage('`');
--得到资讯对应的名称
data_article_name = load '$input_article_path' using PigStorage('`') As (articleId:chararray,articleName:chararray);


--统计pv和uv(日志类型（article）;用户标识(唯一);用户ID;渠道编码标识;资讯ID;资讯地区编码(all所有地区);商家id;创建时间)
alldata_article_pv = load '$input_log_article_path'  using PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,articleId:chararray,areaCode:chararray,shopId:chararray,createTime:chararray);
sp_alldata_article_pv = foreach alldata_article_pv generate articleId,userIdOnly,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray);
--对资讯统计pv
fch_alldata_article_pv = foreach sp_alldata_article_pv generate articleId,dtime,userIdOnly;
grp_fch_alldata_article_pv = group fch_alldata_article_pv by (articleId,dtime);
fch_grp_fch_alldata_article_pv = foreach grp_fch_alldata_article_pv generate flatten(group) As (articleId:chararray,createDate:chararray),COUNT(fch_alldata_article_pv) As pv:long;
--store fch_grp_fch_alldata_article_pv into 'hdfs://h1:9000/lirftest/article_pv';

-- 对资讯统计uv
fch_alldata_article_uv = distinct fch_alldata_article_pv;
grp_fch_alldata_article_uv = group fch_alldata_article_uv by (articleId,dtime);
fch_grp_fch_alldata_article_uv = foreach grp_fch_alldata_article_uv generate flatten(group) As (articleId:chararray,createDate:chararray),COUNT(fch_alldata_article_uv) As uv:long;
--store fch_grp_fch_alldata_article_uv into 'hdfs://h1:9000/lirftest/article_uv';

--统计收藏数据(日志类型（collect）;用户标识(唯一);用户ID;渠道编码标识;收藏途径(1-H5收藏);收藏URL地址;资讯ID;创建时间)
alldata_collect = load '$input_log_collect_path' using PigStorage('`') As (collectId:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,collectFrom:chararray,url:chararray,articlleId:chararray,createTime:chararray);
fch_alldata_collect = foreach alldata_collect generate articlleId,userIdOnly;
dis_fch_alldata_collect = distinct fch_alldata_collect;
grp_dis_fch_alldata_collect = group dis_fch_alldata_collect by articlleId;
fch_grp_dis_fch_alldata_collect = foreach grp_dis_fch_alldata_collect generate flatten(group) As articleId:chararray,COUNT(dis_fch_alldata_collect) As collectCount:long;
--store fch_grp_dis_fch_alldata_collect into 'hdfs://h1:9000/lirftest/article_collect';

-- 统计分享数据(日志类型（share）;用户标识(唯一);用户ID;渠道编码标识;分享途径(1微信浏览器);分享页面URL;分享标题;创建时间)
alldata_share = load '$input_log_share_path' using PigStorage('`') As (shareId,userIdOnly,userId,channelId,shareFrom,url,shareTitle,createTime);
--保留只包括有资讯标识的分享
fi_alldata_share = filter alldata_share by url matches '.*news/.*';
fch_fi_alldata_share_ = foreach fi_alldata_share generate userIdOnly,flatten(STRSPLIT(url,'news/')) As(furl:chararray,articleId:chararray);
fch_fi_alldata_share = foreach fch_fi_alldata_share_ generate userIdOnly,flatten(STRSPLIT(articleId,'\\u003F')) As(articleId:chararray,urlo:chararray);
fch_fch_fi_alldata_share = foreach fch_fi_alldata_share generate articleId,userIdOnly;
grp_fch_fch_fi_alldata_share = group fch_fch_fi_alldata_share by articleId;
fch_grp_fch_fch_fi_alldata_share = foreach grp_fch_fch_fi_alldata_share generate flatten(group) As articleId:chararray,COUNT(fch_fch_fi_alldata_share) As collectCount:long;
--store fch_grp_fch_fch_fi_alldata_share into 'hdfs://h1:9000/lirftest/article_share';

--连接数据
--连接pv+uv
--fch_grp_fch_alldata_article_pv=articleId+createDate+pv
--fch_grp_fch_alldata_article_uv=articleId+createDate+uv
join_pv_uv = join fch_grp_fch_alldata_article_pv by articleId left outer,fch_grp_fch_alldata_article_uv by articleId;
--store join_pv_uv into 'hdfs://h1:9000/lirftest/test1';
--连接collectCount
--join_pv_uv=articleId+createDate+pv+articleId+createDate+uv
--fch_grp_dis_fch_alldata_collect=articleId+collect_count
join_pv_uv_collectCount = join join_pv_uv by $0 left outer,fch_grp_dis_fch_alldata_collect by $0;
--store join_pv_uv_collectCount into 'hdfs://h1:9000/lirftest/test2';
--连接shareCount
--join_pv_uv_collectCount=articleId+createDate+pv+articleId+createDate+uv+articleId+collect_count
--fch_grp_fch_fch_fi_alldata_share=articleId+share_count
join_pv_uv_collectCount_shareCount = join join_pv_uv_collectCount by $0 left outer,fch_grp_fch_fch_fi_alldata_share by $0;
--store join_pv_uv_collectCount_shareCount into 'hdfs://h1:9000/lirftest/test3';
--连接资讯名称
--join_pv_uv_collectCount_shareCount=articleId+createDate+pv+articleId+createDate+uv+articleId+collect_count+articleId+share_count
--data_article_name=articleId+articleName
join_data1 = join join_pv_uv_collectCount_shareCount by $0 left outer,data_article_name by $0;
--store join_data1 into 'hdfs://h1:9000/lirftest/test4';
--连接资讯渠道
--join_data1=articleId+createDate+pv+articleId+createDate+uv+articleId+collect_count+articleId+share_count+articleId+articleName
--dw_data_grp_zc=articleId+channels
join_data2 = join join_data1 by $0 left outer,dw_data_grp_zc by $0;
--store join_data2 into 'hdfs://h1:9000/lirftest/test5';
--连接资讯地区
--join_data2=articleId+createDate+pv+articleId+createDate+uv+articleId+collect_count+articleId+share_count+articleId+articleName+articleId+channels
--dw_data_grp_za=articleId+areaCodes
join_data3 = join join_data2 by $0 left outer,dw_data_grp_za by $0;
--store join_data3 into 'hdfs://h1:9000/lirftest/test6';
--------fch_join_data3 = foreach join_data3 generate $0,$1,$2,$3,$4,$5,$6,$9;
--连接资讯类别
--join_data3=articleId+createDate+pv+articleId+createDate+uv+articleId+collect_count+articleId+share_count+articleId+articleName+articleId+channels+articleId+areaCodes
--dw_data_grp_zt=articleId+types
join_data4 = join join_data3 by $0 left outer,dw_data_grp_zt by $0;
--store join_data4 into 'hdfs://h1:9000/lirftest/test7';
--连接资讯店铺
--join_data4=articleId+createDate+pv+articleId+createDate+uv+articleId+collect_count+articleId+share_count+articleId+articleName+articleId+channels+articleId+areaCodes+articleId+types
--dw_data_grp_zs=articleId+shops
join_data5 = join join_data4 by $0 left outer,dw_data_grp_zs by $0;
--store join_data5 into 'hdfs://h1:9000/lirftest/test8';
--articleId+createDate+pv+articleId+createDate+uv+articleId+collect_count+articleId+share_count+articleId+articleName+articleId+channels+articleId+areaCodes+articleId+types+articleId+shops
result= foreach join_data5 generate $0,$11,(($2 is null)?0L:$2),(($5 is null)?0L:$5),(($7 is null)?0:$7),(($9 is null)?0L:$9),$13,$15,$17,$19,$1;
--结果=资讯ID+资讯名称+pv+uv+收藏数+分享数+渠道+地区+类型+店铺+时间
store result into '$out_result' using PigStorage('`');






