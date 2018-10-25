# ---
# title   : 210_DM_PL_MD
# version : 1.0
# author  : 2e consulting
# desc.   : DM 대출계약 이관 및 템프성테이블(TT) 삭제
# ---

#-------------------
# PL_PLC DM이관
#-------------------
# 현재 테이블 및 위치 정보
WRK_table <- "PL_PLC_TT"
WRK_dir   <- file.path(gsub(HDFS_Path,"",MDEL_WRK_Path),WRK_table)

DM_table  <- "DM_PL_PLC_BASIC"
DM_dir    <- file.path(gsub(HDFS_Path,"",MDEL_PL_Path),DM_table)

# 기존 parquet 파일 삭제
deleteExistParquetFile(DM_dir, DM_table, looping_ar)

# WRK에서 MDEL로 DM 이관
src <- grep(".parquet", hdfs.ls(WRK_dir)$file, value=TRUE)
dest <- gsub(WRK_table, DM_table, gsub(WRK_dir, DM_dir, src))
for(p in seq(src)) hdfs.mv(src=src[p], dest=dest[p])

# 생성한 테이블에 대해서 사용자들의 권한 추가
if(isTRUE(chmod_777)) system(paste0("hdfs dfs -chmod -R 777 ", MDEL_PL_Path,"/",DM_table))

# 관련된 모든 템프성 테이블 삭제
hdfs.rm(WRK_dir)
print(paste0("<<< ",DM_table," - DONE!! >>>"))

# delete table name, dir path
rm(WRK_table)
rm(WRK_dir)
rm(DM_table)
rm(DM_dir)

#-------------------
# PL_PTY DM이관
#-------------------
# 현재 테이블 및 위치 정보
WRK_table <- "PL_PTY_TT"
WRK_dir   <- file.path(gsub(HDFS_Path,"",MDEL_WRK_Path),WRK_table)

DM_table  <- "DM_PL_PTY_BASIC"
DM_dir    <- file.path(gsub(HDFS_Path,"",MDEL_PL_Path),DM_table)

# 기존 parquet 파일 삭제
deleteExistParquetFile(DM_dir, DM_table, looping_ar)

# WRK에서 MDEL로 DM 이관
src <- grep(".parquet", hdfs.ls(WRK_dir)$file, value=TRUE)
dest <- gsub(WRK_table, DM_table, gsub(WRK_dir, DM_dir, src))
for(p in seq(src)) hdfs.mv(src=src[p], dest=dest[p])

# 생성한 테이블에 대해서 사용자들의 권한 추가
if(isTRUE(chmod_777)) system(paste0("hdfs dfs -chmod -R 777 ", MDEL_PL_Path,"/",DM_table))

# 관련된 모든 템프성 테이블 삭제
hdfs.rm(WRK_dir)
print(paste0("<<< ",DM_table," - DONE!! >>>"))

# delete table name, dir path
rm(WRK_table)
rm(WRK_dir)
rm(DM_table)
rm(DM_dir)



