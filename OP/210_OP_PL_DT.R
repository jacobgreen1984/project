# ---
# title   : 210_OP_PL_DT
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 운영 데이터 생성
# ---

# 현재 테이블 및 위치정보
PLC_table <- "OP_PL_PLC_BASIC"
PLC_dir   <- file.Path(gsub(HDFS_Path,"",MDEL_PL_Path),PLC_table)
PTY_table <- "OP_PL_PTY_BASIC"
PTY_dir   <- file.path(gsub(HDFS_Path,"",MDEL_PL_Path),PTY_table)

# hdfs - MDEL - DM 테이블 리드
OP_PL_PLC_BASIC <- read.df(PLC_dir)
createOrReplaceTempView(OP_PL_PLC_BASIC,"OP_PL_PLC_BASIC")
OP_PL_PTY_BASIC <- read.df(PTY_dir)
createOrReplaceTempView(OP_PL_PTY_BASIC,"OP_PL_PTY_BASIC")

# load data in list
OP_PL_PLC_LIST <-	list()
OP_PL_PTY_LIST <-	list()

tryCatch(
  for (i in 1:length(looping_ar)) {
    # set the time variables
    # R노트북 혹은 batch의 parameter를 받아 변수를 일괄적으로 생성
    v_BAS_YM1	<- looping_ar[i]	#	기준년월(M시점)
    
    # read PL_PLC table
    OP_PL_PLC <- SparkR::sql(paste0("
      SELECT	  A1.*
      FROM	    OP_PL_PLC_BASIC	A1
      WHERE	    A1.BAS_YM	     =	",v_BAS_YM1,"
      ORDER BY	A1.PONO
    "))
    
    #	read PL_PTY table
    OP_PL_PTY <- SparkR::sql(paste0("
      SELECT	  A1.*
      FROM	    OP_PL_PTY_BASIC	A1
      WHERE	    A1.BAS_YM	     =	",v_BAS_YM1,"
      ORDER BY	A1.PLHD_INTG_CUST_NO
    "))
    
    # make to DF
    print(paste0("<< NOW : ",PLC_table," to RDT - ", looping_ar[i], " >>")) 
    OP_PL_PLC_LIST[[i]] <- SparkR::as.data.frame(OP_PL_PLC)
    print(paste0("<< NOW : ",PTY_table," to RDT - ", looping_ar[i], " >>")) 
    OP_PL_PTY_LIST[[i]] <- SparkR::as.data.frame(OP_PL_PTY)
  }
  # 생성 중, 에러 처리 
  ,error=function(e) {
    print(paste0("#### ERROR : ",looping_ar[i],"  ####"))
    break()
  }
    )
print('Reading tables done~!')

# stack to DF
OP_PL_PLC <- rbindlist(OP_PL_PLC_LIST)
OP_PL_PTY <- rbindlist(OP_PL_PTY_LIST)

# rename the features in DM_PL_PTY from ??? to CST_???
Merge_Keys < which(colnames(OP_PL_PTY) %in% c("BAS_YM", "PLHD_INTG_CUST_NO"))
colnames(OP_PL_PTY)[-Merge_Keys] <- paste0("CST_",colnames(OP_PL_PTY)[-Merge_Keys])

# merge
setkey(OP_PL_PLC,BAS_YM,PLHD_INTG_CUST_NO)
setkey(OP_PL_PTY,BAS_YM,PLHD_INTG_CUST_NO ) 
dataXY <- OP_PL_PTY[OP_PL_PLC]

# save memory 
rm(OP_PL_PLC) 
rm(OP_PL_PTY) 
rm(OP_PL_PLC_LIST) 
rm(OP_PL_PTY_LIST) 
rm(OP_PL_PLC_BASIC) 
rm(OP_PL_PTY_BASIC)

# print Log
cat("dataXY returned!","\n")
gc(reset - T)