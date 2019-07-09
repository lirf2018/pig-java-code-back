REGISTER /home/hadoop/lrfdata/jars/CleanArticleSql.jar;
DEFINE CleanArticleSql com.uhuibao.pigreljar.CleanArticleSql();
-- 清洗导出的脚本
-- 处理脚本信息(资讯,资讯名称,类别Id,类别名称,店铺id,店铺名称)的sql
alldata_zts = load '$input_log_path1' using PigStorage('^') As (allStrszts:chararray);
dis_alldata_zts = distinct alldata_zts;
fch_dis_alldata_zts = foreach dis_alldata_zts generate flatten(CleanArticleSql(allStrszts));
-- 保存格式为(资讯;资讯名称;类别Id;类别名称;店铺;店铺名称)的清洗数据
store fch_dis_alldata_zts into '$out_ttyh_sqldata_zts' ;

-- 对同一个资讯归类,得到对应的资讯类别组输出tuple(资讯,资讯名称,类别Id) 注:非简单数据
fch_need_datazt = foreach fch_dis_alldata_zts generate $0,$1,$2;
dis_fch_need_datazt = distinct fch_need_datazt;
grp_dis_fch_need_datazt = group dis_fch_need_datazt by ($0,$1);
store grp_dis_fch_need_datazt into '$out_ttyh_sqldata_grp_zt';

-- 对同一个资讯归类,得到对应的资讯店铺组输出tuple(资讯,资讯名称,店铺Id) 注:非简单数据
fch_need_datazs = foreach fch_dis_alldata_zts generate $0,$1,$4;
dis_fch_need_datazs = distinct fch_need_datazs;
grp_dis_fch_need_datazs = group dis_fch_need_datazs by ($0,$1);
store grp_dis_fch_need_datazs into '$out_ttyh_sqldata_grp_zs';

-- 清洗导出的sql脚本
-- 处理脚本信息(资讯,资讯名称,渠道id,渠道编码,渠道名称)的sql数据
alldata_zc = load '$input_log_path2' using PigStorage('^') As (allStrszc:chararray);
dis_alldata_zc = distinct alldata_zc;
fch_dis_alldata_zc = foreach dis_alldata_zc generate flatten(CleanArticleSql(allStrszc));
-- 保存格式为(资讯;资讯名称;渠道id;渠道编码;渠道名称)的清洗数据
store fch_dis_alldata_zc into '$out_ttyh_sqldata_zc' ;

-- 对同一个资讯归类,得到对应的资讯渠道组输出tuple(资讯,资讯名称,渠道编码) 注:非简单数据
fch_need_datazc = foreach fch_dis_alldata_zc generate $0,$1,$3;
grp_fch_need_datazc = group fch_need_datazc by ($0,$1);
store grp_fch_need_datazc into '$out_ttyh_sqldata_grp_zc';

-- 处理脚本信息(资讯,资讯名称,地区id,地区编码,地区名称)的sql
alldata_za = load '$input_log_path3' using PigStorage('^') As (allStrsza:chararray);
dis_alldata_za = distinct alldata_za;
fch_dis_alldata_za = foreach dis_alldata_za generate flatten(CleanArticleSql(allStrsza));
-- 保存格式为(资讯;资讯名称;地区id;地区编码;地区名称)的清洗数据
store fch_dis_alldata_za into '$out_ttyh_sqldata_za' ;

-- 对同一个资讯归类,得到对应的资讯渠道组输出tuple(资讯,资讯名称,地区编码) 注:非简单数据
fch_need_dataza = foreach fch_dis_alldata_za generate $0,$1,$3;
grp_fch_need_dataza = group fch_need_dataza by ($0,$1);
store grp_fch_need_dataza into '$out_ttyh_sqldata_grp_za';


-- 保存渠道静态数据
fch_static_channel = foreach fch_dis_alldata_zc generate $3,$4;
dis_fch_static_channel = distinct fch_static_channel;
store dis_fch_static_channel into '$out_ttyh_sqldata_static_channel' ;
-- 保存地区静态数据
fch_static_area = foreach fch_dis_alldata_za generate $3,$4;
dis_fch_static_area = distinct fch_static_area;
store dis_fch_static_area into '$out_ttyh_sqldata_static_area' ;
-- 保存类别静态数据
fch_static_type = foreach fch_dis_alldata_zts generate $2,$3;
dis_fch_static_type = distinct fch_static_type;
store dis_fch_static_type into '$out_ttyh_sqldata_static_type' ;
-- 保存商家静态数据
fch_static_shop = foreach fch_dis_alldata_zts generate $4,$5;
dis_fch_static_shop = distinct fch_static_shop;
store dis_fch_static_shop into '$out_ttyh_sqldata_static_shop' ;
--保存资讯静态数据
fch_static_article = foreach fch_dis_alldata_zts generate $0,$1;
dis_fch_static_article = distinct fch_static_article;
store dis_fch_static_article into '$out_ttyh_sqldata_static_article' ;




