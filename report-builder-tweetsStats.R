#!/usr/bin/env Rscript

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

library(xtable)
library(knitr)
library(methods) 

library(googleVis)
op <- options(gvis.plot.tag='chart')

args <- commandArgs(TRUE)
if (length(args) < 3) stop("Bad args, usage title input output")

title <- args[1]
source_path <- args[2]
target_path <- args[3]
top <- as.integer(args[4])
include.tweets <- ifelse(is.na(args[4]), TRUE, FALSE)

template_file = "report-template-tweetsStats.Rhtml"

page = readChar(template_file, file.info(template_file)$size)
page = gsub("__TITLE__", title, page)

get_from_hadoop <- function(filename, sort=TRUE, top=NA) {
   fullfilename=file.path(source_path, filename, "part-00000?op=OPEN")
   print(sprintf("Opening file %s", fullfilename))
   df = tryCatch(read.delim(fullfilename, header=FALSE),  error=function(e) data.frame(V1=NA, V2=NA))
   if(sort) df = df[with(df, order(-V1,V2)),] 
   if(!is.na(top)) df = df[1:min(top,nrow(df)),] 
   return(data.frame(V1=paste(df$V2, ""), V2=df$V1))
}

hashtags =get_from_hadoop("hashtags", top=top)
mentions =get_from_hadoop("users_mentions", top=top)
media =get_from_hadoop("media", top=6)
users =get_from_hadoop("users", top=top)
reply_to_user =get_from_hadoop("reply_to_user", top=top)
sources =get_from_hadoop("sources", top=top)
languages =get_from_hadoop("langs", top=top)
tweets.by.day =get_from_hadoop("tweets_by_day", sort=FALSE, top=NA)
#tweets = "tweets", "part-00000?op=OPEN"), header=FALSE)
#tweets$link = sprintf('<a href="https://twitter.com/%s/status/%s">%s</a>', tweets$V2, tweets$V3, tweets$V1)
#tweets.merged <- as.data.frame(paste(tweets$V2, tweets$V1, tweets$V4, sep=" : "))
#M=gvisLineChart(tweets.by.day, xvar="V1", yvar="V2")
tweets.by.day = tweets.by.day[with(tweets.by.day, order(V1)),]
M=gvisLineChart(tweets.by.day, options=list(width=600, 
                                            height=250,
                                            legend=NA,
                                            title="tweets by day"))
knit(text=page, output=target_path)
