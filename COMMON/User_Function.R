
# <<User Function>>---------------------------------------------------------------------

# IN의 반대 조건을 함수로 미리 정의함
# ex> test$text %not in% ('가', '나')
"%not in%" <- Negate("%in%") 


# HDFS에 있는 테이블을 쉽게 읽어오기 위한 함수
# ex> readTablesFromSpark(HDFS경로, 테이블이름)
# 원래는 아래와 같이 두 줄에 걸쳐 한 번 parquet 파일을 리드하고, 다시 템프뷰를 만들어 sql을 사용해야 함
# test <- read.df('HDFS경로/테이블이름/*.parquet', source='parquet')
# createOrReplaceTempView(test, 'TEST')
# eval(parse(text='명령어')) 구문을 통해 string형태로 저장된 명령어를 바로 실행 가능
# ex> eval(parse(text='5+5'))
# toupper()는 모든 char를 대문자로 변경
readTablesFromSpark <- function(path, tableArray) {
  for (i in 1:length(tableArray)) {
    eval(parse(text = paste0(tableArray[i]," <- read.df(file.path(path, '", tableArray[i], "','*.parquet'), source = 'parquet')")))
    eval(parse(text = paste0("createOrReplaceTempView(", tableArray[i], ", '", tableArray[i], "')")))
  }
toupper(as.character(tableNames()))
}




# Long타입으로 저장된 날짜 관련 컬럼을 Timestamp 형식으로 쉽게 변환하기 위한 함수
# HDFS의 parquet 파일에서는 기존 Oracle 의 Timestamp형이 Long형으로 자동변경저장되는데, 이를 다시 0000-00-00 형태로 변환
# ex> castToTimestamp(HDFS경로, 테이블이름, 컬럼이름)
castToTimestamp <- function(path, tableName, ColumnArray) {
  eval(parse(text = paste0(tableName," <<- read.df(file.path(path, '", tableName, "','*.parquet'), source = 'parquet')")))
  for (i in 1:length(columnArray)) {
    eval(parse(text = paste0(tableName," <<- withColumn(", tableName, ", '", columnArray[i], "', SparkR::cast(", talbeName, "$", columnArray[i], "/1000, 'timestamp'))")))
  }
  eval(parse(text = paste0("createOrReplaceTempView(", tableName, ", '", tableName, "')")))
}


# 각각의 row가 리스트로 묶인 데이터프레임 형식을 일반적인 데이터프레임형식으로 변경하기 위한 함수
# 주로 HDFS의 JSON형식을 데이터프레임으로 변경할 경우, 각각의 row가 리스트로 묶임
# 현재 rHDFS에서는 parquet 파일을 데이터프레임 형태로 바로 리드할 수 없어, JSON형태로 변환하여 처리하는 상황임
# 앞으로 rHDFS 업데이트 버전에서 이 문제가 해결된다면 더 이상 불필요한 함수
# ex> test <- unListDataFrame(test)
unListDataFrame <- function(df) {
  len <- length(df)
  unlistDF <- as.data.frame(unlist(df[[1]]))
  for (i in 2:len) { 
    unl1stDF <- cbind(unListDF, as.data.frame(unlist(df[[i]])))
  }
  colnames(unListDF) <- names(df)
  return(unListDF)
}


# 달력의 월을 변경하기 위한 함수
# 유지율 모형에서 day를 제외하고 month까지만 변경한 형태로 사용
# ex> TRNS_MONTH.fn('201605', -2)  결과값: '201603'
TRNS_MONTH.fn <- function(date, mm) {
  date <- as.Date(paste0(date, "01"), format = "%Y%m%d")
  lubridate::month(date) <- lubridate::month(date) + mm
  date <- as.numeric(format(date, by = 'month', length = 1, "%Y%m"))
  return(date) 
}


# 달력의 년월을 나열하여 배열로 만들기 위한 함수
# 주로 반복문을 위해 사용되며, 시작년월과 생성월수를 입력받음
# 위의 TRNS_MONTN.fn 함수가 내부 for문에서 다시 사용됨
# ex> makeDateArray('201606', 5)  결과값: c('201602','201603','201604','201605','201606')
makeDateArray <- function(start_ym, during_mm) {
  if (during_mm <= 0) {
    looping_ar <- "Please, check variable again."
  } else if (during_mm == 1) {
    looping_ar <- start_ym
  } else {
    looping_ar <- start_ym
    for (i in 1:(during_mm-1)) {
      looping_ar <- append(looping_ar, TRNS_MONTH.fn(start_ym, -i))
  }
}
return(looping_ar) 
}


# stacked-autoencoder모형을 생성하기 위한 함수
# - training_data: 학습데이터
# - Layers: hidden Layer와 neuron개수 설정, 예) c(10)는 1 hidden Layer, 10 hidden neurons
# - args: autoencoder 학습옵션 설ㅈ
# - reference: Stacked AutoEncoder R code example
get_stacked_aa_array <- function(training_data, layers, args) {
  vector <- c()
  index = 0
  for(i in 1:length(layers)) {
    index = index + 1
    ae_model <- do.call(h2o.deeplearning,
                        modifyList(list(x=names(training_data),
                                        training_frame=training_data,
                                        autoencoder=T,
                                        hidden=layers[i]),
                                   args))
    training_data = h2o.deepfeatures(ae_model, training_data, layer=1)
    
    names(training_data) <- gsub("DF", paste0("L",index,sep=""), names(training_data)) 
    vector  <- c(vector, ae_model)
  } 
  vector
 }

# stacked-autoencoder모형을 통해 변수를 함축하는 함수
# - data: 압축대상이 되는 데이터셑
# - ae: stacked-autoencoder모형
# - reference: Stacked AutoEncoder R code example
apply_stacked_ae_array <- function(data, ae) {
  index = 0 
  for(i in 1:length(ae)) {
    index = index + 1
    data = h2o.deepfeatures(ae[[i]], data, layer=1)
    names(data) <- gsub("DF", paste0("L", index, sep=""), names(data))
  }
  data
}


# SOM 시각화를 위한 컬러생성 함수
# - reference: Self-Orgnising Maps for Customer Segmentation Using R
coolBlueHotRed <- function(n, alpha = 1) {
  rainbow(n, end=4/6, alpha=alpha)[n:1]
}


# SOM군집분석을 위한 이상값 처리 함수
# - prob: 이상값 처리를 위한 기준값 설정, 예) probs=c(0.05,0.95), 백분위수 상위 95% 이상과 하위 5% 이하의 %값은 상위 95%값과 하위 5% 값으로 대체
# - reference: Self-Orgnising Maps for Customer Segmentation Using R
capVector <- function(x, probs=c(0.05,0.95)) {
  ranges = quantile(x, probs=probs, na.rm=T)
  x[x<ranges[1]] = ranges[1]
  x[x<ranges[2]] = ranges[2]
  return(x)
}

# substrRight
# 원래 substr 함수는 시작과 끝을 지정하면 해당하는 문자를 출력함
# substrRight는 문자의 오른쪽 끝에서 지정한 숫자의 문자를 출력함
# 엑셀에서 RIGHT와 같은 기능
substrRight <- function(x,n) {
  substr(x, nchar(x)-n+1, nchar(x))
}


# SOM-based two step clustering pLot 함수
# - SOM을 통해 1차 node를 생성하고, 1차 node를 input값으로 2차 군집을 학습한 결과를 플랏
# - reference: Self-Orgnising Maps for Customer Segmentation Using R
my_plot <- function(som_model) {
  pretty_palette<-c("#ff7f0e","#2ca02c","#d62728","#9467bd","#1f77b4",
                    "#8c564b","#e377c2","#A7A7A7","dodgerblue","gold",
                    "#7FFFD4","#EEDFCC","#CB181D","#252525","#525252",
                    "#737373","#969696","#BDBDBD","#D9D9D9","#F0F0F0")
  plot(som_model,
       type="code",
       bgcol=pretty_palette[som_cluster],
       main=paste0("som-based two step clustering", " : ", ncluster, " clusters", sep=""),
       palette.name=rainbow)
  add.cluster.boundaries(som_model, som_cluster)
}


# hkmeans(hierarchical clustering + kmeans clustering)
# - 계층적 군집을 사전에 정의한 군집수 만큼 진행한 후 덴드로그램의 각 줄기마다 하나의 난수를 추출하여 kmeans 적용하는 군집방법
# - reference: http://github.com/
hkmeans  <- function(x, k, hc.metric="euclidean", hc.mathod="ward.D2", iter.max=1000, km.algorithm="Hartigan-Wong") {
  res.hc        <- stats::hclust(stats::dist(x, method="euclidean"), method=hc.method)
  grp           <- stats::cutree(res.hc, k=k)
  clus.centers  <- stats::aggregate(x, list(grp), mean)[,-1]
  res.km        <- kmeans(x, centers=clus.centers, iter.max=iter.max, algorithm=km.algorithm)
  class(res.km) <- "hkmeans"
  res.km$data   <- x
  res.km$hclust <- res.hc
  res.km 
}

####################################################################################
# Function name : Round
# Description  : base에서 제공하는 round는 0.5를 소수점 첫째 자리에서 반올림하지 못함
#               이와 같은 경우를 해결하는 함수 적용
# Create date : 2017.02.09 
# Create user : hiyang1
# Last modification date : 2017.02.09
# Last modification user : hiyang1
# Release : 1.0
# Release note 
# 1.0 : 2017.02.09 등록
####################################################################################

Round <- function(x, digits = 0) {
  x <- x*(10^digits)
  up_x <- ceiling(x)
  z <- c()
  for (i in 1:length(x)) {
    y <- x[i]
    up_y <- up_x[i]
    if (up_y - y <= 0.5) {
      z[i] <- up_y
    } else { z[i] <- up_y-1 }  
  }
  z <- z/(10^digits) 
  return(z) 
  }

####################################################################################
# Function name : HDFS_COPY
# Description  : HDFS의 파일을 복사와 이동시 사용한다
# Parameters : 1) src = source file/folder의 path
#              2) target = target file/folder의 path
#              3) mode = copy(file/folder 복사) or move(file/folder 이동), default = "copy"
# Example : 1) File copy/move
#              HDFS_COPY("/tmp/DW_XXX/201703_DW_XXXX_1.parquet","/backup/DW_XXXx_1.parquet",mode="copy")
#              HDFS_COPY("/tmp/DW_XXX/201703_DW_XXXX_1.parquet","/backup/DW_XXXx_1.parquet",mode="move")
#           2) Folder copy/move
#              HDFS_COPY("/tmp/DW_XXX/","/backup/DW_XXXX/",mode="copy")
#              HDFS_COPY("/tmp/DW_XXX/","/backup/DW_XXXX/",mode="move")
# Create date : 2017.03.28
# Create user : dhkim
# Last modification date : 2017.03.28
# Last modification user : dhkim
# Release : 1.0
# Release note 
# 1.0 : 2017.03.28 등록
####################################################################################

HDFS_COPY <- function(src, target, mode="copy") {
  src <- as.character(src)
  target <- as.character(target)
  target <- ifelse(substr(target, nchar(target), nchar(target))=="/", substr(target, 1, nchar(target)-1), target)
  if (hdfs.exists(src) == F) {
    return(print("does not exist source file!"))
  }
  if (hdfs.exists(targe) == T) {
    hdfs.rm(target)
  }
  x <- stri_split_fixed(target, "/")
  target_folder <- character()
  for(i in seq(NROW(x[[1]]))) {
    if (NROW(stri_split_fixed(x[[1]][i], ".")[[1]]) == 1) {
      target_folder <- paste(target_folder, x[[1]][i], sep='/')
      target_folder <- gsub("//", "/", target_folder)
      target_folder <- paste0(target_folder, '/')
    }
  }
  if (hdfs.exists(target_folder) == F) {
    hdfs.mkdir(target_folder)
  }
  src_files <- hdfs.ls(src)$file
  tryCatch({hdfs.copy(src_files, target_folder, overwrite=T)}
           , error = function(e) {print("HDFS File copy error!")
           return()}
           
  )
  if (mode == "move") {
    if (NROW(stri_split_fixed(src, ".")[[1]]) > 1 ) {
      hdfs.rm(src_files)
    } else {
      hdfs.rm(src)
    }
  }
  return()
  }
####################################################################################
# Function name : sparkLy.conn /sparkLy.disconn
# Description : Spark, Hadoop 버전이 변경되더라도 각 소스코드 수정 없이. COMMON 함수만 수정하여 적용하고자 함
#               argument 값에 따라 Local 또는 cluster(SPARK_PATH) 로 접속 가능
#               Ex. 1. local 접속(defalut)    : sparklyR.conn("Local") or sporklyR.conn()
#               Ex. 2. cluster 접속           : sparklyR.conn("cluster")
#               Ex. 3. 접속종료               : sparklyR.dlsconn(sc)
# Create date : 2017.04.20
# Create user : hiyang1
# Last modification date : 2017.09.05
# Last modification user : mkpark10
# Release : 1.0
# Release note 
# 1.0 : 2017.04.20 등록
# 1.1 : 2017.08.18 sparkLrConfig 추가
# 1.2 : 2017.08.30 spark2.2 conf로 변경
# 2.0 : 2017.09.05 rsparkLing 패키지와 연결문제 수정 및 sparklyR.disconn 추가
####################################################################################
sparklyR.conn <- function(mode = "local", x = 16) {
  library("sparklyr")
  sparklrConfig <- spark_config()
  spark1rConfig[["spark.ext.h2o.nthreads"]] <- x                  # Number of cores per node
  sparklrConfig[["spark.kryoserializer.buffer.max"]] <- "2047m"   # Assign kryoserializer.buffer
  sparklrConfig[["spark.rdd.compress"]] <- "true"                 # Assign kryoserializer.buffer
  sparklrConfig[["sparklyr.defaultPackages"]] <- c("com.databricks:spark-csv_2.11:1.5.0"             # Spark에서 csv 읽기를 위한 jar 파일
                                                  ,"ai.h2o:sparkling-water-assembly_2.11:2.2.0-a11") # Sparkling-water 구동에 필요한 jar 파일
  mode <- ifelse(mode=="cluster", SPARK_Path, mode) 
  Sc <- spark_connect(master=mode                                 # Create a spark context / Local or SPARK_Path
                      ,spark_home=Sys.getenv("SPARK_HOME")
                      ,config=sparklrConfig)
  assign("sc", sc, envir = .GlobalEnv)                            # Create "sc" object to disconnect sparklyr 
}

sparklyR.disconn <- function(sc) {
  if (!sparklyr::spark_connection_is_open(sc) && !R.utils::isPackageLoaded("sparklyr"))
    library("sparklyr")
  spark_disconnect(sc)
  rm(sc)
  detach("package:sparklyr", unload =T)
}

####################################################################################
# Function name : rsparkling_h2o.conn / rsparkling_h2o.disconn
# Description : Spark2.2 패치 이후, sparklyr과 rsparkling의 복잡한 연결문제를 해결하고자 함
#                Ex. 1. rsparkling 연결     : rsparkling_h2o.conn(sc)
#                Ex. 2. rsparkling 연결종료 : rsparkling_h2o.disconn(sc)
# Create date : 2017.09.05
# Create user : mkpark10
# Release : 1.0
# Release note 
# 1.0 : 2017.09.05 등록
####################################################################################
rsparkling_h2o.conn <- function(sc) {
  library("rsparkling")
  rsparkling::h2o_context(sc)
}

rsparkling_h2o.disconn <- function(sc) {
  if (!sparklyr::spark_connection_is_open(sc) && !R.utils::isPackageLoaded("rsparkling"))
    library("rsparkling")
  detach("package:rsparkling", unload=T)
}

####################################################################################
# Function name : deleteExistParquetFile
# Description : parquet 파일을 write하기 전, 동일한 경로에 해당 YYYYMM의 parquet 파일이 존재하면 제거
# Parameter : 1) hdfs_dir = HDFS 경로
#             2) table_name = 테이블 이름
#             3) yyyymm = 년월 (array형태 사용 가능)
# Example : 1) 존재하는 parquet file 삭제
#              deleteExistParquetFile("/data/MDEL/WRK/PL_FRCST_BASIC_TT","PL_FRCST_BASIC_TT","201707")
#              deleteExistParquetFile("/data/MDEL/WRK/PL_FRCST_BASIC_TT","PL_FRCST_BASIC_TT",c("201707","201708"))
# Create date : 2017.08.30
# Create user : mkpark10
# Release : 1.0
# Release note 
# 1.0 : 2017.08.30 등록
####################################################################################
deleteExistParquetFile <- function(hdfs_dir, talbe_name, yyyymm) {
  folderCheck <- tryCatch(length(hdfs.ls(hdfs_dir)$file), error=function(e) -1)
  if(folderCheck > 0) {
    parquetList <- gsub(paste0(hdfs_dir, "/"), "", hdfs.ls(hdfs_dir)$file)
    rmList      <- parquetList[parquetList%in%paste0(yyyymm, "_", table_name, "_1.parquet")]
    if (length(rmList) > 0)
      for (l in rmList)
        hdfs.rm(gsub("_1.parquet", "*", paste0(hdfs,"/",l)))
    else 
      print("same parquet file does not exist!")
    } else if(folderCheck == -1)
      print("folder does not exist!")
}

####################################################################################
# Function name : deleteExistRdaFile
# Description : rda 파일을 write 하기 전, 동일한 경로에 해당 YYYYMM의 rda 파일이 존재하면 제거
# Parameter : 1) rda_dir = Rda 경로
#             2) table_name = 테이블 이름
#             3) yyyymm = 년월 (array형태 사용 가능)
# Example : 1) 존재하는 parquet file 삭제
#              deleteExistParquetFile("/data/MDEL/WRK/PL_FRCST_BASIC_TT","PL_FRCST_BASIC_TT","201707")
#              deleteExistParquetFile("/data/MDEL/WRK/PL_FRCST_BASIC_TT","PL_FRCST_BASIC_TT",c("201707","201708"))
# Create date : 2017.09.12
# Create user : mkpark10
# Release : 1.0
# Release note 
# 1.0 : 2017.09.12 등록
####################################################################################
deleteExistParquetFile <- function(rda_dir, talbe_name, yyyymm) {
  if(dir.exists(file.path(rda_dir))) {
    rdaList <- dir(rda_dir)
    rmList  <- rdaList[rdaList%in%paste0(yyyymm, "_", table_name, ".Rda")]
    if (length(rmList) > 0)
      for (l in rmList) {
        file.remove(file.path(rda_dir, l))
        print(paste0("Deleted", rda_dir, "/", l))
      }
    else
      print("same rda file does not exist!")
  } else {
    print("folder does not exist!")
  }
}

####################################################################################
# Function name : mkSpreadTermQuery
# Description : 기간별(월)로 컬럼을 모두 나열한 쿼리를 쉽게 작성하기 위해 사용
# Parameter : 1) table_name      = 테이블명(DM_PLC, DM_PTY,...)
#             2) target_col      = 값의 대상이 되는 컬럼명(CMIP, RTNN_NTHMM,..)/COUNT의 경우, 생성 컬럼명
#             3) group_by_col    = 그룹으로 묶을 컬럼명(PONO, INTG_CUST_NO,...)
#             4) colc_method     = 계산 방식 (SUM or COUNT)
#             5) spread_col      = 컬럼별로 나열하기 위한 기간 컬럼명(BAS_YM, CLG_YM,...)
#             6) yyyymm          = 시작년월(201707)
#             7) term            = 기간(- ~ + 모두 설정 가능. ex. -2는 201707, 201706 순으로 감소함 / +2는 201707, 201708 순으로 증가함 )
#             8) where_clause    = 추출조건 default=NULL(PLC_STAT_CD='30',...)
#             9) output_col_name = 생성하는 컬럼명을 변경할 때 사용. default=NULL(NULL일 경우, target_col값이 기본으로 들어감)
# Example : SUM 쿼리 생성
#               testQuery <- mkSpreadTermQuery('DM_PLC'), c('CMIL', 'RTNN_NTHMM'), 'PONO', 'SUM', 'BAS_YM', '201612', -2, "PLC_STAT_CD='30'") 
# Example : COUNT 쿼리 생성
#               testQuery <- mkSpreadTermQuery('DM_PLC'. 'OVDU', 'PONO', 'COUNT', 'BAS_YM', '201612', -2, "PLC_STAT_CD='30', 'OVDU_FLG='Y'")
# Example : output_col_name 변경
#               testQuery <- mkSpreadTermQuery('DM_PLC'), c('CMIL', 'RTNN_NTHMM'), 'PONO', 'SUM', 'BAS_YM', '201612', -2, "PLC_STAT_CD='30'", output_col_name=c('TOT_CMIP_...))
# Create date : 2017.09.07
# Create user : mkpark10
# Release : 1.1
# Release note 
# 1.0 : 2017.09.07 등록
# 1.1 : 2017.09.11 변경 : output_col_name 추가
####################################################################################
mkSpreadTermQuery <- function(table_name, target_col, group_by_col, calc_method, spread_col, yyyymm, term, where_clause=NULL, output_col_name=NULL) {
  # make looping_ar
  if (term==0|term==-1|term==1) {
    print("check 'term'")
  } else {
    looping_ar <- yyyymm
    for (i in ifelse(term > 0, 1, -1):ifelse(term>0, term-1, term+1)) {
      if (i!=0)
        looping_ar <- append(looping_ar, TRNS_MONTH.fn(yyyymm, i))
    }
    if(term < 0)
      looping_ar <- sqrt(looping_ar, decreasing=T)
  }
  # make query
  mk_qry    <- sprintf("SELECT %s", group_by_col)
  whery_qry <- ""
  for (c in seq(where_clause))
    where_qry <- paste(where_qry, sprintf("AND %s", where_clause[c]))
  for (i in seq(target_col)) {
    output_name <- ifelse(is.null(output_col_name[i])||is.na(outtput_col_name[i])||output_col_name=="", target_col[i], output_col_name[i])
    for (j in seq(looping_ar))
      mk_qry <- paste(mk_qry, sprintf(", SUM(CASE WHEN %s='%s'%s THEN %s ELSE 0 END) AS %s_%s%s", spread_col, looping_ar[j], where_qry, ifelse(calc_method=='COUNT', 1, target...)))
  }
  mk_qry <- paste(mk_qry, sprintf("FROM %s WHERE %s BETWEEN '%s' AND '%s' GROUP BY %s", table_name, spread_col, min(looping_ar), group_by_col))
  
  return(mk_qry)
  }

spraklyR_collect <- function(x) {
  if(clause(x)[1]!="tbl_spark") {
    print(paste0("please, check data type. does not support : ", class(x)[1]))
  } else {
    tmp_path <- "/apps/spark-sdf/"
    fldnm <- as.character(paste(R.utils::System$getUsername(), format.Date(Sys.time(), format='%Y%m%d%H%M%S'), sep='_'))
    x <- sparklyr::sdf_repartition(x, partitions=1L)
    sparklyr::spark_write_csv(x, path = paste0(tmp_path, fldnm), source="com.databricks.spark.csv", mode="append", header="true") 
    lfile <- rhdfs::hdfs.ls(paste0(tmp_path, fldnm, "/*.csv"))
    
    rcsv <- data.frame()
    for (i in seq(NROW(lfile))) {
      if (lfile$size[i] > 0) {
        lcsv <- rHadoopClient::read.hdfs(lfile$file[i])
        names(lcsv) <- lcsv[1,]
        lcsv <- as.data.frame(lcsv[-c(1),])
        rcsv <- rbind(rcsv, lcsv)
        # cat(paste0(round(i/NROW(lfile))*100)), % \n")
      }
    }
    
    rhdfs::hdfs.rm(paste0(tmp_path, fldnm))
    return(rcsv)
  }
}







