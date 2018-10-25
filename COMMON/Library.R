# Library
# EXCEL 파일을 읽고 쓰는데 사용
library(xlsx)
library(readxl)
library(openxlsx)

# 기본적인 데이터 처리에 특화된 펑션들을 가지고 있는 패키지 
library(dplyr)
library(reshape2)
library(ggplot2)

#  data.table class 를 사용 
library(data.table)

# sql을 이용해 데이터를 핸들링하는데 사용
library(sqldf)

# HDFS에 접근하는데 사용 
library(rhdfs)
hdfs.init()       # rHDFS는 사용하기 전에 반드시 init을 해줘야 함

# SparkR은 서버의 SPARK_MONE에 있는 R 라이브러리를 직접 읽어서 사용
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# JSON형태의 파일을 읽고 쓰는데 사용 
library(rjson)

# 병렬 혹은 분산병렬 작업을 수행하기 위한 패키지 
library(snow)

# 날짜 변경을 쉽게 하기 위해 사용되는 패키지 
library(lubridate)

# SOM 병렬학습을 위한 패키지 
library(Rsomoclu)

# SOM 시각화를 위한 패키지 
library(kohonen)

# SOM heatmap 플랏에서 명목형변수를 binary로 변환하기 위한 패키지 
library(dummies)

# silhouette플랏을 위한 패키지
library(cluster)

# Fuzzy C-means clustering 패키지
library(e1071)

# 데이터 전처리
library(stringi)
