args <- commandArgs(TRUE)
if (length(args) < 4) stop("Bad args, usage hdfspath report period stats")

hdfspath="/user/r/staging/twitter/searches"
period="2014/12/11"
stats="Stats"
stats=""
top=20

hdfspath=args[1]
year=args[2]
month=args[3]
day=args[4]
report=args[5]
top=as.integer(args[6])
email=args[7]
tweets=args[8]

if(!is.na(tweets) && tweets != '') {
  tweets = "--tweets"
} else {
  tweets = ""
}

period=sprintf("%s/%s", year, month)
if(! day=="*") period=sprintf("%s/%s", period, day)

period_with_slash=period
if(day=="*") period_with_slash=sprintf("%s/", period_with_slash)


dir=basename(hdfspath)
period_for_title=gsub("/","-", period)
hdfspath_target=sprintf("/tmp/twitter/%s/%s/%s", dir, report, period)
path_target=sprintf("/var/www/twitter/%s/%s/%s", dir, report, period)

command=sprintf("hdfs dfs -rm -r %s/[a-z]*", hdfspath_target)
print(command)
system(command)

command=sprintf("hdfs dfs -mkdir -p %s", hdfspath_target)
system(command)
command=sprintf("/bin/mkdir -p %s", path_target)
system(command)

title=sprintf("Twitter/%s/%s %s", dir, report, period)
html_target=sprintf("/var/www/twitter/%s/%s/%s/%s%s_%s.html", dir,report,period,report,stats,period_for_title)


command=sprintf('spark-submit --master yarn-client extractTweets.py --source "%s/%s/%s*.gz" --target %s %s', hdfspath, report, period_with_slash, hdfspath_target, tweets)
print(command)
system(command)

command=sprintf('Rscript report-builder-tweets.R "%s" "http://localhost:50070/webhdfs/v1%s" %s %s %s', title, hdfspath_target, html_target, top, tweets)
print(command)
system(command)

if(!is.na(email) && email != '') {
  command=sprintf('echo "" |mailx -s "%s_%s" -a %s %s', dir, report, html_target, email)
  system(command)
}
