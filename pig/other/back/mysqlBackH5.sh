#!/bin/bash
source /etc/profile

#DB服务器IP
db_host="192.168.0.49"
#database name
db_name="huicloud_info_db"
#database username
db_user="root"
#database password
db_pass="test@123"

#当前日期时间字符串 例：2010-12-20 （年月日时分秒）
date=`date +%Y-%m-%d`
filedate=`date +%Y%m%d`
#执行mysql命令的参数
sql_opt="-u$db_user -p$db_pass -h $db_host $db_name"

###创建文件夹
dire=/usr/local/mysqlBack/article

if [ ! -d "$myPath"]; then  
　　mkdir ${dire}  
fi
chmod 777 ${dire}



#文件保存路径
filePath=/usr/local/mysqlBack/

/usr/local/mysql/bin/mysql $sql_opt << EOF
#将单个表的数据导出到文件中，
#FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' 这些选项是把数据用逗号分隔，双引号引起来，换行用\n;如果不用，可以将这些选项删除。
#####资讯类别店铺(资讯Id,资讯名称,类别id,类别名称,店铺Id,店铺名称)
SELECT t1.news_id, t1.news_title, t2.category_id, t3.cate_name, t4.shop_id, t5.shop_name FROM tb_info_news t1 LEFT JOIN tb_info_category_rela t2 ON t1.news_id=t2.thing_id LEFT JOIN tb_info_new_category t3 ON t2.category_id=t3.category_id LEFT JOIN tb_info_news_shop t4 ON t1.news_id=t4.news_id LEFT JOIN tb_info_shop t5 ON t4.shop_id=t5.shop_id into outfile "${filePath}/article/article_shop_type${filedate}.txt" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

#####资讯地区(资讯Id,资讯名称,地区id,地区编码,地区名称)
SELECT t1.news_id, t1.news_title, t3.area_id, t2.area_code, t3.area_name FROM tb_info_news t1 LEFT JOIN tb_info_news_area_rela t2 ON t1.news_id=t2.news_id LEFT JOIN tb_info_area t3 ON t3.area_code = t2.area_code into outfile "${filePath}/article/article_area${filedate}.txt" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

#####资讯渠道(资讯Id,资讯名称,渠道id,渠道编码,渠道名称)
SELECT t1.news_id, t1.news_title, t2.chan_id, t3.chan_code, t3.chan_name FROM tb_info_news t1 LEFT JOIN tb_info_news_channel t2 ON t1.news_id=t2.news_id LEFT JOIN tb_info_channel t3 ON t2.chan_id=t3.chan_id into outfile "${filePath}/article/article_channel${filedate}.txt" FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

quit
EOF

echo 'Backup success'