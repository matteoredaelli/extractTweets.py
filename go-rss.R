args <- commandArgs(TRUE)
if (length(args) < 3) stop("Bad args, usage hdfsbase_target period")

hdfsbase_source="/user/r/staging/rss"
hdfsbase_target="/tmp/rss"

year=args[1]
month=args[2]
day=args[3]
email=args[4]
news=args[5]
top=10

if(!is.na(news) && news != '') {
  news="dummy"
} else {
  news = ""
}

period_month=sprintf("%s/%s", year, month)
period=sprintf("%s/%s/%s", year, month, day)

period_for_title=gsub("/","-", period)
#period_for_title=gsub("*","all", period)
web_path_target=file.path("/var/www/rss", period_for_title)

hdfs_source=file.path(hdfsbase_source, period)
hdfs_target=file.path(hdfsbase_target, period_for_title)

command=sprintf("hdfs dfs -rm %s/*/*", hdfs_target)
system(command)
command=sprintf("hdfs dfs -rmdir %s/*", hdfs_target)
system(command)
command=sprintf("hdfs dfs -rmdir %s", hdfs_target)
system(command)

title=sprintf("Web News %s", period)
html_target=sprintf("%s_web_news.html", web_path_target)

command=sprintf('spark-submit --master yarn-client extract-rss.py --source %s --target %s --year %s --month %s --day %s', hdfsbase_source, hdfs_target, year, month, day)
print(command)
system(command)

command=sprintf('http_proxy= Rscript report-builder-rss.R "%s" "http://localhost:50070/webhdfs/v1%s" %s %s %s', title, hdfs_target, html_target, top, news)
print(command)
system(command)

if(!is.na(email) && email != '') {
  command=sprintf('echo "" |mailx -s "%s" -a %s %s', period_month, html_target, email)
  print(command)
  system(command)
}
