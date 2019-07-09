# -*- coding: utf-8 -*-    
#mysqldb    
import sys
import time, MySQLdb    
reload(sys)
sys.setdefaultencoding('utf-8')


##时间
now=time.strftime("%Y%m%d", time.localtime())
#print now

#连接
#conn=MySQLdb.connect(host="192.168.0.110",port=3888,user="datasync",passwd="datasync20170114",db="huicloud_info_db",charset="utf8")
conn=MySQLdb.connect(host="192.168.0.49",user="root",passwd="test@123",db="huicloud_info_db",charset="utf8")  
cursor = conn.cursor()
#rootPath="/usr/local/mysqlBack"
rootPath="/home/hadoop/lrfdata/mysqlBack"


1####查询资讯类别店铺(资讯Id`资讯名称`类别id`类别名称`店铺Id`店铺名称)
#保存值
value=""
listTest=[]
article_type_shop_sql="SELECT t1.news_id, t1.news_title, t2.category_id, t3.cate_name, t4.shop_id, t5.shop_name FROM tb_info_news t1 LEFT JOIN tb_info_category_rela t2 ON t1.news_id=t2.thing_id LEFT JOIN tb_info_new_category t3 ON t2.category_id=t3.category_id LEFT JOIN tb_info_news_shop t4 ON t1.news_id=t4.news_id LEFT JOIN tb_info_shop t5 ON t4.shop_id=t5.shop_id"
n1 = cursor.execute(article_type_shop_sql)
for row in cursor.fetchall():    
        if value !="":
          listTest.append(value+"\n")
        value=""
	for r in row:      
           value=value+str(r)+"`"
listTest.append(value+"\n")
fileName=rootPath+"/ttyh/article/article_shop_type"+now+".txt"
#print fileName
file_object = open(fileName, 'w+')
file_object.writelines(listTest)
file_object.close()

2#####资讯地区(资讯Id`资讯名称`地区id`地区编码`地区名称)
value=""
listTest=[]
article_area_sql="SELECT t1.news_id, t1.news_title, t3.area_id, t2.area_code, t3.area_name FROM tb_info_news t1 LEFT JOIN tb_info_news_area_rela t2 ON t1.news_id=t2.news_id LEFT JOIN tb_info_area t3 ON t2.area_code = t2.area_code"
n2=cursor.execute(article_area_sql)
for row in cursor.fetchall():
	if value !="":
	  listTest.append(value+"\n")
    	value=""
	for r in row:
	  value=value+str(r)+"`"
listTest.append(value+"\n")
fileName=rootPath+"/ttyh/article/article_area"+now+".txt"
file_object = open(fileName, 'w+')
file_object.writelines(listTest)
file_object.close()


3#####资讯渠道(资讯Id`资讯名称`渠道id`渠道编码`渠道名称)
value=""
listTest=[]
article_channel_sql="SELECT t1.news_id, t1.news_title, t2.chan_id, t3.chan_code, t3.chan_name FROM tb_info_news t1 LEFT JOIN tb_info_news_channel t2 ON t1.news_id=t2.news_id LEFT JOIN tb_info_channel t3 ON t2.chan_id=t3.chan_id"
n3=cursor.execute(article_channel_sql)
for row in cursor.fetchall():
	if value !="":
	  listTest.append(value+"\n")
	value=""
	for r in row:
	  value=value+str(r)+"`"
listTest.append(value+"\n")
fileName=rootPath+"/ttyh/article/article_channel"+now+".txt"
file_object = open(fileName, 'w+')
file_object.writelines(listTest)
file_object.close()


4#####卡券商家渠道类型(卡券Id`卡券名称`库存`开始时间`结束时间`商家Id`商家名称`渠道id`渠道编码`渠道名称`类型id`类型名称)
value=""
listTest=[]
ticket_shop_channel_type_sql="SELECT t1.coupon_id, t1.coupon_name, t2.coupon_spec_stock,t1.coupon_start_date, t1.coupon_end_date, t1.shop_id, t3.shop_name, t5.chan_id, t5.chan_code, t5.chan_name, t7.category_id, t7.cate_name FROM tb_coupon t1 JOIN tb_coupon_spec t2 ON t1.coupon_id=t2.coupon_id LEFT JOIN tb_info_shop t3 ON t1.shop_id=t3.shop_id JOIN tb_coupon_channel t4 ON t1.coupon_id=t4.coupon_id JOIN tb_info_channel t5 ON t4.chan_id=t5.chan_id JOIN tb_coupon_category_rela t6 ON t1.coupon_id=t6.coupon_id JOIN tb_info_new_category t7 ON t6.category_id=t7.category_id ORDER BY t1.coupon_id ASC"
n4=cursor.execute(ticket_shop_channel_type_sql)
for row in cursor.fetchall():
	if value !="":
	  listTest.append(value+"\n")
	value=""
	for r in row:
	  value=value+str(r)+"`"
listTest.append(value+"\n")
fileName=rootPath+"/ttyh/article/ticket_shop_channel_type"+now+".txt"
file_object = open(fileName, 'w+')
file_object.writelines(listTest)
file_object.close()


5#####商家卡片渠道(卡片Id`卡片编码`卡片名称`渠道id`渠道编码`渠道名称`地区id`地区编码`地区名称)
value=""
listTest=[]
shop_channel_area_sql="SELECT t1.shop_id, t1.shop_name, t1.shop_code, t2.chan_id, t3.chan_code, t3.chan_name,t4.area_id, t5.area_code, t5.area_name FROM tb_info_shop t1 JOIN tb_info_shop_channel t2 ON t1.shop_id=t2.shop_id LEFT JOIN tb_info_channel t3 ON t2.chan_id=t3.chan_id JOIN tb_info_shop_area t4 ON t1.shop_id=t4.shop_id LEFT JOIN tb_info_area t5 ON t4.area_id=t5.area_id"
n5=cursor.execute(shop_channel_area_sql)
for row in cursor.fetchall():
	if value !="":
	  listTest.append(value+"\n")
	value=""
	for r in row:
	  value=value+str(r)+"`"
listTest.append(value+"\n")
fileName=rootPath+"/ttyh/article/shop_channel_area"+now+".txt"
file_object = open(fileName, 'w+')
file_object.writelines(listTest)
file_object.close()


6#####服务类别(服务Id`服务名称`类别id`类别名称)
value=""
listTest=[]
service_type_sql="SELECT t1.service_id, t1.service_name, t2.category_id, t3.cate_name FROM tb_service t1 JOIN tb_service_category t2 ON  t1.service_id=t2.service_id LEFT JOIN tb_info_new_category t3 ON t2.category_id=t3.category_id"
n5=cursor.execute(service_type_sql)
for row in cursor.fetchall():
	if value !="":
	  listTest.append(value+"\n")
	value=""
	for r in row:
	  value=value+str(r)+"`"
listTest.append(value+"\n")
fileName=rootPath+"/ttyh/article/service_type"+now+".txt"
file_object = open(fileName, 'w+')
file_object.writelines(listTest)
file_object.close()

