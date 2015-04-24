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
#   spark-submit  --master yarn-client extract-data-from-words.py --source_twitter "/user/r/staging/twitter/searches/tyre/2014/12/*.gz" --target /tmp/tests/15

## output
##  (todo picture_url), source, link, text
import json
import re
import sys
import time
import os,argparse

import xml.etree.cElementTree as ET

from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SQLContext

# twitter

def javaTimestampToString(t):
  return time.strftime("%Y-%m-%d", time.localtime(t/1000))

def cleanText(text):
  t = re.sub('["\']', ' ', unicode(text))
  return t.replace("\n"," ").replace("\t", " ").replace("\r", " ").replace("  ", " ")

def build_tweet_url(screenName, id):
  return "https://twitter.com/%s/status/%s" % (screenName, id)
# end twitter 

## for rrs

def html_to_text(html):
  try:
    return re.sub(r"<.*?>", "", html)
  except:
    return html

def rss_string_to_xml_object(line):
  line = line.encode('utf8', 'replace')
  tree = ET.ElementTree(ET.fromstring(line))
  return tree.getroot()

def safe_root_find(root, field):
  try:
    return root.find(field).text
  except:
    return ""

def rss_string_to_list(line):
  root = rss_string_to_xml_object(line)
  title = cleanText(safe_root_find(root, 'title'))
  description = cleanText(html_to_text(safe_root_find(root, 'description')))
  ##pubDate = safe_root_find(root, 'pubDate')[5:16]
  ## carbuzz has not pubDate field... :-(
  ##if pubDate == "":
  ##	pubDate = "no date"
  ##else:
  ## 	pubDate = datetime.strptime(pubDate, '%d %b %Y').strftime("%Y-%m-%d")

  source = safe_root_find(root, 'rss_source')
  link = safe_root_find(root, 'link')
  ##language = safe_root_find(root, 'rss_language')
  ##category = safe_root_find(root, 'rss_category')
  return (source,link ,title + ": "+ description)          

##end rss

if __name__ == "__main__":

  ####################################
  ## parsing command line parameters
  ####################################
  parser = argparse.ArgumentParser()

  ## es /user/r/staging/rss/2015/02/02.gz
  parser.add_argument("--source_rss", help="source path for rss")
  ## es /user/r/staging/twitter/searches/TheCalExperience.json/2015/02/02.gz
  parser.add_argument("--source_twitter", help="source path for twitter")
  parser.add_argument("--target", help="target path")
  parser.add_argument("--word", help="word")

  args = parser.parse_args()

  word = args.word
  source_path_rss = args.source_rss
  source_path_twitter = args.source_twitter
  target_path = args.target + "/" + word
  word = word.upper()

  ####################################
  ## spark setup
  ####################################
  sc = SparkContext(appName="extract-data-by-words.py")
  sqlContext = SQLContext(sc)
  
  ##########################
  ## extraxt tweets
  ##########################
  tweets = sqlContext.jsonFile(source_path_twitter)
  tweets.registerTempTable("tweets")

  sql = "SELECT distinct user.screenName, id, text FROM tweets where upper(text) like '%%%s%%'" % word
  print sql
  rdd_twitter = sqlContext.sql(sql)
  #text.filter = rdd.filter(lambda t: t[2] and word in t[2].upper())
  rdd_twitter = rdd_twitter.map(lambda t: ("twitter", build_tweet_url(t[0],t[1]), t[0] + ": " + unicode(cleanText(t[2]))))

  ##########################
  ## extract rss
  ##########################
  rdd_rss = sc.textFile(source_path_rss).distinct().filter(lambda t: word in t.upper()).map(rss_string_to_list)
  
  ##########################
  ## saving
  ##########################
  rdd = rdd_twitter.union(rdd_rss)
  rdd.map(lambda x: '\t'.join(unicode(i).replace("\n"," ") for i in x)).repartition(1).saveAsTextFile(target_path)

  ##########################
  ## quitting
  ##########################
  sc.stop()


