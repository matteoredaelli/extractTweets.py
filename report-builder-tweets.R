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

args <- commandArgs(TRUE)
if (length(args) < 3) stop("Bad args, usage title input output")

title <- args[1]
source_path <- args[2]
target_path <- args[3]

template_file = "report-template-tweets.Rhtml"

page = readChar(template_file, file.info(template_file)$size)
page = gsub("__TITLE__", title, page)

get_from_hadoop <- function(filename) {
   fullfilename=file.path(source_path, filename, "part-00000?op=OPEN")
   print(sprintf("Opening file %s", fullfilename))
   df = tryCatch(read.delim(fullfilename, header=FALSE),  error=function(e) data.frame(V1=NA, V2=NA))
   return(df)
}

tweets = get_from_hadoop("tweets")
tweets$link = sprintf('<a href="https://twitter.com/%s/status/%s">%s</a>: %s', tweets$V2, tweets$V3, tweets$V2, tweets$V1)
#tweets.merged <- as.data.frame(paste(tweets$V2, tweets$V1, tweets$V4, sep=" : "))
tweets.merged <- data.frame(tweets$link)
knit(text=page, output=target_path)
