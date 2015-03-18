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
include.news <- ifelse(is.na(args[5]) || args[5]=='', FALSE, TRUE)

template_file = "report-template-rss.Rhtml"

page = readChar(template_file, file.info(template_file)$size)
page = gsub("__TITLE__", title, page)

get_from_hadoop <- function(filename, sort=TRUE, top=NA) {
   fullfilename=file.path(source_path, filename, "part-00000?op=OPEN")
   print(sprintf("Opening file %s", fullfilename))
   df = tryCatch(read.delim(fullfilename, header=FALSE, quote=""),  error=function(e) data.frame(V1=NA, V2=NA))
   if(sort) df = df[with(df, order(-V1,V2)),] 
   if(!is.na(top)) df = df[1:min(top,nrow(df)),] 
   return(df)
}

swap.df <- function(df) {
   return(data.frame(Item=paste(df$V2, ""), Count=df$V1))
}


category = swap.df(get_from_hadoop("category", top=top))
language = swap.df(get_from_hadoop("language", top=top))
source = swap.df(get_from_hadoop("source", top=top))
news.by.day =swap.df(get_from_hadoop("pubDate", sort=FALSE, top=NA))

if(include.news) {
  rss = get_from_hadoop("news", sort=FALSE)
  # (language,source,category,pubDate,title,link,description)          
  rss <- subset(rss, !is.null(V5) & !is.na(V5) & V5 != '') 
  news = sprintf('<h2>%s (%s)</h2><i>%s, %s</i><br />%s', rss[,5], rss[,3], rss[,2], rss[,4], rss[,7])
  if(! rss[,6] == "") news = sprintf("%s <a href=%s>details</a>", news, rss[,6])
  news.merged=data.frame(news=news)
} else {
  news.merged=NULL
}

#M=gvisLineChart(news.by.day, xvar="V1", yvar="V2")
news.by.day = news.by.day[with(news.by.day, order(Item)),]
news.by.day.chart=gvisLineChart(news.by.day, options=list(width=600, 
                                            height=250,
                                            legend='none',
                                            title="news by day"))
default.options=list(width=200, allowHtml=TRUE)

knit(text=page, output=target_path)
