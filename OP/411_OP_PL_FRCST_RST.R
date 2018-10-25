# ---
# title   : 411_OP_PL_FRCST_RPT
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 운영 리포트 생성
# ---


#	predict with newdata
BEST_ML_PRED <- as.data.frame(predict(BEST_ML, newdata=test))

# make PL_OP_OUTPUT
OUTPUT_FEATURES = C("BAS_YM",
                    "PLHD_INTG_CUST_NO",
                    "NML_INTRT",
                    "PONO",
                    "SRDVLU_WAMT",
                    "TOT_PL_TIMES_MM11.CUMSUM",
                    "PL_PSB_AMT")

# 대출경험 여부
PL_OP_OUTPUT <- dataXY[,OUTPUT_FEATURES, with=F]
PL_OP_OUTPUT[,P0:=BEST_ML_PRED[,2]]
PL_OP_OUTPUT[,P1:=BEST_ML_PRED[,3]]

# make group (!!1등급의 대출확율이 가장 높게 시작!!))
PL_OP_OUTPUT[, GROUP := as.numeric(Hmisc::cut2(P0, g=20))]
PL_OP_OUTPUT[, GROUP := sprintf("%02d", GROUP)]

# apply round
PL_OP_OUTPUT[,P0:=round(P0, 7)]
PL_OP_OUTPUT[,P1:=round(P1, 7)]

# compute PSI
computePIS = tryCatch(length(hdfs.ls(file.path(MDEL_PL_Path, "OP_PL_FRCST_RST"))$file)>0, error=function(e) return(FALSE))

if(isTRUE(computePIS)){
  
  #run make PSI with OLD and NEW dataset
  PL_OP_OUTPUT_OLD = read.df(file.path(HDFS_Path, head(sort(hdfs.ls(file.path(MDEL_PL_Path, "OP_PL_FRCST_RST"))$file, decreasing=F), 1)))
  cat(">> For PSI index, the BAS_YM", unique(as.data.frame(PL_OP_OUTPUT_OLD[,"BAS_YM"])$BAS_YM), "will be used as OLD_DATA!", "\n")
  
  PL_OP_OUTPUT_NEW = PL_OP_OUTPUT
  PL_OP_PSI <- makePSI(OLD_DATA = PL_OP_OUTPUT_OLD, NEW_DATA = PL_OP_OUTPUT_NEW)
  
  # compute PSI >> save PSI to HDFS
  PL_OP_PSI <- as.h2o(PL_OP_PSI)
  MDEL_EVAL_RST_HDFS_Path <- file.path(HDFS_Path, " /data/MDEL/PL_FRCST/OP_PL_FRCST_PSI")
  h2o.exportHDFS(PL_OP_PSI, path=file.path(MDEL_EVAL_RST_HDFS_Path, paste0(looping_ar, "_OP_PL_FRCST_PSI.csv")), force=T)
  if(isTRUE(chmod_777)) system(paste0("hdfs dfs -chmod -R 777 ", file.path(MDEL_EVAL_RST_HDFS_Path, paste0(looping_ar, "_OP_PL_FRCST_PSI.csv"))))
  print(hdfs.ls(MDEL_EVAL_RST_HDFS_Path))
  } else cat(">> PSI can NOT be computed because no file in OP_PL_FRCST_RST to compute with!", "\n")

# convert sparkR.data.frame
OP_PL_FRCST_RST <- as.DataFrame(PL_OP_OUTPUT)

# save PL_OP_OUTPUT IN WK
WRK_table <- 'OP_PL_FRCST_RST'
file_path <- MDEL_WRK_Path

# 기존 parquet 파일 삭제
deleteExistParquetFile(file.path(gsub(HDFS_Path, "", MDEL_WRK_Path), WRK_table), WRK_table, looping_ar)

# parquet 파일 생성
print(paste0(">> NOW :", WRK_table, " - ", looping_ar, " >>"))
assign(WRK_table, repartition(get(WRK_table), numPartitions=1L))
write.df(get(WRK_table), file.path(file.path, WRK_table), source="parquet", mode="append")

# 저장된 parquet 파일명을 변경하기
WRK_parquet  <- hdfs.ls(path = file.path(file_path, WRK_table))$file
parquet_name <- WRK_parquet[grep('.snappy.parquet', WRK_parquet)]
hdfs.rename(parquet_name, paste0(file_path, "/", WRK_table, "/", looping_ar, "_", WRK_table, "_1.parquet"))

# move OP_PL_FRCST_RST in MDEL
MDEL_table <- "OP_PL_FRCST_RST"
MDEL_dir   <- file.path(gsub(HDFS_Path, "", MDEL_PL_Path), MDEL_table)
src        <- grep(".parquet", hdfs.ls(file.path(file_path, WRK_table))$file, value=T)
dest       <- file.path(MDEL_dir, gsub(".*\\RST/", "", src))
hdfs.mv(src=src, dest=dest)
print(paste0(">> MOVE : ", MDEL_table, " - ", looping_ar, " >>"))

# WK 폴더 파일 제거
hdfs.rm(file.path(MDEL_WRK_Path, "OP_PL_FRCST_RST"))

# 생성된 테이블에 대해서 사용자들(user, group, everyone)의 권한(read, write, delete) 추가
if(isTRUE(chmod_777)){
  system(paste0("hdfs dfs -chmod -R 777 ", MDEL_EVAL_RST_HDFS_Path))
  system(paste0("hdfs dfs -chmod -R 777 ", MDEL_dir))
}

# print Log
cat(">> OP_PL_FRCST_RST done!", "\n") 







