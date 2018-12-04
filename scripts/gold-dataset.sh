#!/bin/bash
#Author: Akhil Jain
# Basic Shell Script to fetch/pull hdfs file to local and
# copy to tmp. This could be later used to push to artifactory
# for gold dataset.
#
tenant='thun_expn.db'
tables=("tbai_quote" "tbai_product_attrs" "tbai_opportunity")
opportunity="tbai_opportunity"
today_date=`date +%Y-%m-%d`
dirc=`mkdir "/tmp/gold-dataset-"${tenant}"-"${today_date}`;
path="/tmp/gold-dataset-"${tenant}"-"${today_date}"/"
echo $path;
for i in "${tables[@]}"
do
        if [ $i != "tbai_opportunity" ];then

                for filename in `hadoop fs -ls /user/hive/warehouse/${tenant}/${i}|grep ${today_date}|grep -v empty|awk '{print $8}';`
                        #sfor filename in `hadoop fs -ls /user/hive/warehouse/thun_expn.db/tbai_quote|grep 2018-12-03|grep -v empty|awk '{print $8}';`
                do
                        echo $filename;
                        hadoop fs -copyToLocal ${filename} ${path};
                done;
        else
                for filename in `hadoop fs -ls /user/hive/warehouse/${tenant}/${i}|grep -v 'empty\|sample'|awk '{print $8}';`
                do
                        echo $filename;
                        hadoop fs -copyToLocal ${filename} ${path};
                done;
        fi
done;
