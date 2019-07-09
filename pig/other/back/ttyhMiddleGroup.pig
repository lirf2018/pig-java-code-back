-- h5日志生成中间数据
------------------------------------------资讯静态数据------------------------------------------
-- 加载资讯类别店铺信息(资讯,资讯名称,类别Id,类别名称,店铺id,店铺名称)
alldata_zts = load '$input_log_path1' using PigStorage('`') As (articleId:chararray,articleName:chararray,typeId:chararray,typeName:chararray,shopId:chararray,shopName:chararray);

-- 对同一个资讯归类,得到对应的资讯类别组输出tuple(资讯,类别Id) 注:非简单数据
fch_need_datazt = foreach alldata_zts generate $0,$2;
dis_fch_need_datazt = distinct fch_need_datazt;
fi_dis_fch_need_datazt = filter dis_fch_need_datazt by $1 != 'None';
grp_dis_fch_need_datazt = group fi_dis_fch_need_datazt by $0;
store grp_dis_fch_need_datazt into '$out_ttyh_sqldata_grp_zt';

-- 对同一个资讯归类,得到对应的资讯店铺组输出tuple(资讯,店铺Id) 注:非简单数据
fch_need_datazs = foreach alldata_zts generate $0,$4;
dis_fch_need_datazs = distinct fch_need_datazs;
fi_dis_fch_need_datazs = filter dis_fch_need_datazs by $1 != 'None';
grp_dis_fch_need_datazs = group fi_dis_fch_need_datazs by $0;
store grp_dis_fch_need_datazs into '$out_ttyh_sqldata_grp_zs';

-- 处理渠道信息(资讯,资讯名称,渠道id,渠道编码,渠道名称)的数据
alldata_zc = load '$input_log_path2' using PigStorage('`') As (articleId:chararray,articleName:chararray,channelId:chararray,channelCode:chararray,channelName:chararray);
-- 对同一个资讯归类,得到对应的资讯渠道组输出tuple(资讯,渠道编码) 注:非简单数据
fch_need_datazc = foreach alldata_zc generate $0,$3;
dis_fch_need_datazc = distinct fch_need_datazc;
fi_dis_fch_need_datazc = filter dis_fch_need_datazc by $1 != 'None';
grp_fch_need_datazc = group fi_dis_fch_need_datazc by $0;
store grp_fch_need_datazc into '$out_ttyh_sqldata_grp_zc';

-- 处理地区信息(资讯,资讯名称,地区id,地区编码,地区名称)的
alldata_za = load '$input_log_path3' using PigStorage('`') As (articleId:chararray,articleName:chararray,areaId:chararray,areaCode:chararray,areaName:chararray);
-- 对同一个资讯归类,得到对应的资讯渠道组输出tuple(资讯,资讯名称,地区编码) 注:非简单数据
fch_need_dataza = foreach alldata_za generate $0,$3;
dis_fch_need_dataza = distinct fch_need_dataza;
fi_dis_fch_need_dataza = filter dis_fch_need_dataza by $1 != 'None';
grp_fch_need_dataza = group fi_dis_fch_need_dataza by $0;
store grp_fch_need_dataza into '$out_ttyh_sqldata_grp_za';


------------------------------------------卡券静态数据------------------------------------------
alldata_tsct = load '$input_log_path4' using PigStorage('`') As (ticketId:chararray,ticketName:chararray,kc:chararray,startDate:chararray,endDate:chararray,shopid:chararray,shopName:chararray,channelId:chararray,channelCode:chararray,channelName:chararray,typeId:chararray,typeName:chararray);
--对同一个卡券归类,得到卡券商家(卡券id,商家id) 注:非简单数据
fch_alldata_tsct_ts = foreach alldata_tsct generate $0,$5;
dis_fch_alldata_tsct_ts = distinct fch_alldata_tsct_ts;
fi_dis_fch_alldata_tsct_ts = filter dis_fch_alldata_tsct_ts by $1 != 'None';
grp_fi_dis_fch_alldata_tsct_ts = group fi_dis_fch_alldata_tsct_ts by $0;
store grp_fi_dis_fch_alldata_tsct_ts into '$out_ttyh_sqldata_grp_ts';
--对同一个卡券归类,得到卡券渠道code(卡券id,渠道code) 注:非简单数据
fch_alldata_tsct_tc = foreach alldata_tsct generate $0,$8;
dis_fch_alldata_tsct_tc = distinct fch_alldata_tsct_tc;
fi_dis_fch_alldata_tsct_tc = filter dis_fch_alldata_tsct_tc by $1 != 'None';
grp_fi_dis_fch_alldata_tsct_tc = group fi_dis_fch_alldata_tsct_tc by $0;
store grp_fi_dis_fch_alldata_tsct_tc into '$out_ttyh_sqldata_grp_tc';
--对同一个卡券归类,得到卡券类型id(卡券id,类型id) 注:非简单数据
fch_alldata_tsct_tt = foreach alldata_tsct generate $0,$10;
dis_fch_alldata_tsct_tt = distinct fch_alldata_tsct_tt;
fi_dis_fch_alldata_tsct_tt = filter dis_fch_alldata_tsct_tt by $1 != 'None';
grp_fi_dis_fch_alldata_tsct_tt = group fi_dis_fch_alldata_tsct_tt by $0;
store grp_fi_dis_fch_alldata_tsct_tt into '$out_ttyh_sqldata_grp_tt';


------------------------------------------商家卡片静态数据------------------------------------------
alldata_sca = load '$input_log_path5' using PigStorage('`') As (shopId:chararray,shopName:chararray,shopCode:chararray,channelId:chararray,channelCode:chararray,channelName:chararray,areaId:chararray,areaCode:chararray,areaName:chararray);
--对同一个卡片归类,得到卡片与渠道code  注:非简单数据
fch_alldata_sca_sc = foreach alldata_sca generate $0,$4;
dis_fch_alldata_sca_sc = distinct fch_alldata_sca_sc;
fi_dis_fch_alldata_sca_sc = filter dis_fch_alldata_sca_sc by $1 != 'None';
grp_fi_dis_fch_alldata_sca_sc = group fi_dis_fch_alldata_sca_sc by $0;
store grp_fi_dis_fch_alldata_sca_sc into '$out_ttyh_sqldata_grp_sc';
--对同一个卡片归类,得到卡片与地区code  注:非简单数据
fch_alldata_sca_sa = foreach alldata_sca generate $0,$7;
dis_fch_alldata_sca_sa = distinct fch_alldata_sca_sa;
fi_dis_fch_alldata_sca_sa = filter dis_fch_alldata_sca_sa by $1 != 'None';
grp_fi_dis_fch_alldata_sca_sa = group fi_dis_fch_alldata_sca_sa by $0;
store grp_fi_dis_fch_alldata_sca_sa into '$out_ttyh_sqldata_grp_sa';


-- 保存渠道静态数据
fch_static_channel_article = foreach alldata_zc generate $3,$4;--从资讯中得到
fch_static_channel_ticket = foreach alldata_tsct generate $8,$9;--从卡券中得到
fch_static_channel_shopcard = foreach alldata_sca generate $4,$5;--从商家卡片中得到
uni_channel_1 = union fch_static_channel_article,fch_static_channel_ticket;
uni_channel_2 = union uni_channel_1,fch_static_channel_shopcard;
--
dis_uni_channel_1 = distinct uni_channel_2;
fi_dis_uni_channel_all = filter dis_uni_channel_1 by $1 != 'None';
store fi_dis_uni_channel_all into '$out_ttyh_sqldata_static_channel' using PigStorage('`');

-- 保存地区静态数据
fch_static_area_article = foreach alldata_za generate $3,$4;--从资讯中得到
fch_static_area_shopcard = foreach alldata_sca generate $7,$8;--从商家卡片中得到
uni_area1 = union fch_static_area_article,fch_static_area_shopcard;
--
dis_fch_static_area = distinct uni_area1;
fi_dis_fch_static_area = filter dis_fch_static_area by $1 != 'None'; 
store fi_dis_fch_static_area into '$out_ttyh_sqldata_static_area' using PigStorage('`');

-- 保存类别静态数据
fch_static_type_article = foreach alldata_zts generate $2,$3;--从资讯中得到
fch_static_type_ticket = foreach alldata_tsct generate $10,$11;--从卡券中得到
uni_type_1 = union fch_static_type_article,fch_static_type_ticket;
--
dis_uni_type_1 = distinct uni_type_1;
fi_dis_uni_type_all = filter dis_uni_type_1 by $1 != 'None';
store fi_dis_uni_type_all into '$out_ttyh_sqldata_static_type' using PigStorage('`');

-- 保存商家静态数据
fch_static_shop_article = foreach alldata_zts generate $4,$5;--从资讯中得到
fch_static_shop_ticket = foreach alldata_tsct generate $5,$6;--从卡券中得到
uni_shop_1 = union fch_static_shop_article,fch_static_shop_ticket;

dis_uni_shop_1 = distinct uni_shop_1;
fi_dis_uni_shop_all = filter dis_uni_shop_1 by $1 != 'None';
store fi_dis_uni_shop_all into '$out_ttyh_sqldata_static_shop' using PigStorage('`');

--保存资讯静态数据
fch_static_article = foreach alldata_zts generate $0,$1;--从资讯中得到
dis_fch_static_article = distinct fch_static_article;
store dis_fch_static_article into '$out_ttyh_sqldata_static_article' using PigStorage('`');

--保存卡券静态数据
fch_static_ticket = foreach alldata_tsct generate $0,$1,$2,$3,$4;--从卡券中得到
dis_fch_static_ticket = distinct fch_static_ticket;
store dis_fch_static_ticket into '$out_ttyh_sqldata_static_ticket' using PigStorage('`');

-- 商家卡片静态数据
fch_alldata_sca_scs = foreach alldata_sca generate $0,$1;
dis_fch_alldata_sca_scs = distinct fch_alldata_sca_scs;
fi_dis_fch_alldata_sca_scs = filter dis_fch_alldata_sca_scs by $0 != 'None' and $1 != 'None';
store fi_dis_fch_alldata_sca_scs into '$out_ttyh_sqldata_static_shopcard' using PigStorage('`');





