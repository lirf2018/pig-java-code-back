#!/bin/bash
now_str=$(date -d now +%Y%m%d%H%m%S)
rootPath=/home/hadoop/lrfdata
touch ${rootPath}/big-data-tool-uhuibao-logs/h5$now_str.txt
java -jar  ${rootPath}/big-data-tool-uhuibao-h5.jar >  ${rootPath}/big-data-tool-uhuibao-logs/h5$now_str.txt

