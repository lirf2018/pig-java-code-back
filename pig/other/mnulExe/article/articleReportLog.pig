
-- 资讯 日志入库
---2017-01-01至2017-03-02 的资讯日志数据（资讯日志没有增加type) 前的脚本
allData_article = load '$input_clean_article_log'  using PigStorage('`')  As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,articleId:chararray,areaCode:chararray,shopId:chararray,createTime:chararray);
fch_allData_article = foreach allData_article generate userIdOnly,userId,articleId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),channelId,areaCode,shopId,'' As type:chararray;

---2017-03-03至 当前  的资讯日志数据（资讯日志增加type) 的脚本
--allData_article = load '$input_clean_article_log'  using PigStorage('`')  As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,articleId:chararray,areaCode:chararray,shopId:chararray,type:chararray,createTime:chararray);
--fch_allData_article = foreach allData_article generate userIdOnly,userId,articleId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),channelId,areaCode,shopId,type;


--pig脚本输出=用户标识（唯一）+用户Id+资讯id+日期+时间+渠道+地区+商家+类别
store fch_allData_article into '$out_clean_article_log' using PigStorage('`');



-- 收藏 日志入库
---2017-01-01至2017-01-19 的收藏日志（收藏日志没有增加店铺id）前的脚本
allData_collect = load '$input_clean_collect_log' using PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,collectFrom:chararray,url:chararray,articleId:chararray,createTime:chararray);
fch_allData_collect = foreach allData_collect generate userIdOnly,userId,articleId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),collectFrom,channelId,'' As shopId:chararray,'' As areaCode:chararray;

---2017-01-20至2017-03-01 的收藏日志（收藏日志增加店铺id）的脚本
--allData_collect = load '$input_clean_collect_log' using PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,collectFrom:chararray,url:chararray,articleId:chararray,shopId:chararray,createTime:chararray);
--fch_allData_collect = foreach allData_collect generate userIdOnly,userId,articleId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),collectFrom,channelId,shopId,'' As areaCode:chararray;

---2017-03-02至 当前 的收藏日志（收藏日志增加地区）的脚本
--allData_collect = load '$input_clean_collect_log' using PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,collectFrom:chararray,url:chararray,articleId:chararray,shopId:chararray,areaCode:chararray,createTime:chararray);
--fch_allData_collect = foreach allData_collect generate userIdOnly,userId,articleId,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),collectFrom,channelId,shopId,areaCode;



----pig脚本输出=用户标识（唯一）+用户Id+资讯id+日期+时间+收藏来源+渠道+店铺id+地区
store fch_allData_collect into '$out_clean_article_collect_log' using PigStorage('`');

-- 分享日志入库
---2017-01-01至2017-03-01 的分享日志 （分享日志没有增加地区） 前的脚本
allData_share = load '$input_clean_share_log' using PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,shareFrom:chararray,url:chararray,shareTitle:chararray,createTime:chararray);
fi_allData_share = filter allData_share by url matches '.*news/.*';
fch_fi_allData_share = foreach fi_allData_share generate userIdOnly,userId,channelId,shareFrom,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),flatten(STRSPLIT(url,'news/')) As(furl:chararray,articleId:chararray);
fch_fch_fi_allData_share = foreach fch_fi_allData_share generate userIdOnly,userId,channelId,shareFrom,dtime,mins,flatten(STRSPLIT(articleId,'\\u003F')) As(articleId:chararray,urlo:chararray);
fch_fch_fch_fi_allData_share = foreach fch_fch_fi_allData_share generate userIdOnly,userId,articleId,dtime,mins,shareFrom,channelId,'' As areaCode:chararray;

---2017-03-02至 当前 的分享日志 （分享日志增加地区） 前的脚本
--allData_share = load '$input_clean_share_log' using PigStorage('`') As (logType:chararray,userIdOnly:chararray,userId:chararray,channelId:chararray,shareFrom:chararray,url:chararray,shareTitle:chararray,areaCode:chararray,createTime:chararray);
--fi_allData_share = filter allData_share by url matches '.*news/.*';
--fch_fi_allData_share = foreach fi_allData_share generate userIdOnly,userId,channelId,shareFrom,areaCode,flatten(STRSPLIT(createTime,' ')) As (dtime:chararray,mins:chararray),flatten(STRSPLIT(url,'news/')) As(furl:chararray,articleId:chararray);
--fch_fch_fi_allData_share = foreach fch_fi_allData_share generate userIdOnly,userId,channelId,shareFrom,areaCode,dtime,mins,flatten(STRSPLIT(articleId,'\\u003F')) As(articleId:chararray,urlo:chararray);
--fch_fch_fch_fi_allData_share = foreach fch_fch_fi_allData_share generate userIdOnly,userId,articleId,dtime,mins,shareFrom,channelId,areaCode;


---pig脚本输出=用户标识（唯一）+用户Id+资讯id+日期+时间+分享来源+渠道+地区
store fch_fch_fch_fi_allData_share into '$out_clean_article_share_log' using PigStorage('`');








