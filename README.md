# extractTweets.py

An apache Spark job for extracting statistics from tweets saved in Hadoop hdfs

## LICENSE

GPL V3+

## Author

Matteo DOT redaelli AT gmail DOT com

http://www.redaelli.org/matteo/

## Usage

spark-submit  --master yarn-client --driver-class-path /path/to/spark/assembly/target/scala-2.10/spark-assembly-1.3.0-SNAPSHOT-hadoop2.5.2.jar extractTweets.py --source "/user/r/staging/twitter/searches/opensource/2014/12/*.gz" --target /tmp/tests/1

http_proxy= Rscript tweets-report-builder.R "opensource" "http://hadoop.redaelli.org:50070/webhdfs/v1/tmp/tests/1" /var/www/opensource.html 20 skip

