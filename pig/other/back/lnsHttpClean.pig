-- 加载数据
alldata = load 'hdfs://h1:9000/tmp/lns/httpLog20170101004310.dat' USING PigStorage(','); 
-- 保留
cleandata = FILTER alldata BY $2 matches '.*h5\\.uhuibao\\.com.*'; 

store cleandata into 'hdfs://h1:9000/clean/lnshttp';