#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# For usage and details, see http://www.gnu.org/licenses/gpl-3.0.txt

# AUTHOR: 
#
#   matteo DOT redaelli AT gmail DOT com
#   http://www.redaelli.org/matteo
#
#
# USAGE:
#
#   spark-submit  --master yarn-client --driver-class-path /path/to/spark/assembly/target/scala-2.10/spark-assembly-1.3.0-SNAPSHOT-hadoop2.5.2.jar extractTweetsStats.py --source "/user/r/staging/twitter/searches/tyre/2014/12/*.gz" --target /tmp/tests/15

import json
import re
import sys
import time

import os,argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext

def javaTimestampToString(t):
  return time.strftime("%Y-%m-%d", time.localtime(t/1000))

def cleanText(text):
  t = re.sub('["\']', ' ', unicode(text))
  return t.replace("\n"," ").replace("\t", " ").replace("  ", " ")

if __name__ == "__main__":

  ## parsing command line parameters
  parser = argparse.ArgumentParser()
  parser.add_argument("--source", help="source path")
  parser.add_argument("--target", help="target path")

  args = parser.parse_args()

  ## connecting to hdfs data
  source_path = args.source # /user/r/staging/twitter/searches/TheCalExperience.json/*/*/*.gz
  sc = SparkContext(appName="extraxtTweets.py")
  sqlContext = SQLContext(sc)
  
  tweets = sqlContext.jsonFile(source_path)
  tweets.registerTempTable("tweets")
  t = sqlContext.sql("SELECT distinct user.screenName, id, text FROM tweets")

  ## extraxt tweets
  
  text = t.map(lambda t: (cleanText(t[2]),t[0],t[1])).map(lambda x: '\t'.join(unicode(i) for i in x)).repartition(1)
       
  ## save stats from tweets to hdfs
  text.saveAsTextFile("%s/%s" % (args.target, "tweets"))
