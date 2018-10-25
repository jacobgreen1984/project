# ---
# title   : 110_ML_PL_DT
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 분석데이터셋 생성
# ---

# 현재 테이블 및 위치정보
PLC_table <- "DM_PL_PLC_BASIC"
PLC_dir   <- file.Path(gsub(HDFS_Path,"",MDEL_PL_Path),PLC_table)
  
PTY_table <- "DM_PL_PTY_BASIC"
PTY_dir   <- file.path(gsub(HDFS_Path,"",MDEL_PL_Path),PTY_table)

# hdfs - MDEL - DM 테이블 리드
DM_PL_PLC.org <-	spark_read_parquet(sc,	"DM_Pl_PLC_BASIC",	PLC_dir)
DM_PL_PTY.org <-	spark_read_parquet(sc,	"DM_PL_PTY_BASIC",	PTY_dir)
                            
# load data in list
DM_PL_PLC_LIST <-	list()
DM_PL_PTY_LIST <-	list()

tryCatch(
  for (i in 1:length(looping_ar)) {
    # set the time variables
    # R노트북 혹은 batch의 parameter를 받아 변수를 일괄적으로 생성
    v_BAS_YM1	<- looping_ar[i]	#	기준년월(M시점)
    
    # read PL_PLC table
    DM_PL_PLC <- ft_sql_transformer(DM_PL_PLC.org ,sql=paste0("
    SELECT	A1.*
    FROM	DM_PL_PLC_BASIC	A1
    WHERE	A1.BAS_YM	     =	",v_BAS_YM1,"
    ORDER BY	A1.PONO
    "))
    
    #	read PL_PTY table
    DM_PL_PTY <- ft_sql_transformer(DM_PL_PTY.org ,sql=paste0("
    SELECT	A1.*
    FROM	DM_PL_PTY_BASIC	A1
    WHERE	A1.BAS_YM	     =	",v_BAS_YM1,"
    ORDER BY	A1.PLHD_INTG_CUST_NO
    "))

    # make to DF
    print(paste0("<< NOW : ",PLC_table," to RDT - ", looping_ar[i], " >>")) 
    DM_PL_PLC_LIST[[i]] <- dplyr::collect(DM_PL_PLC)
    print(paste0("<< NOW : ",PTY_table," to RDT - ", looping_ar[i], " >>")) 
    DM_PL_PTY_LIST[[i]] <- dplyr::collect(DM_PL_PTY)
    }
    # 생성 중, 에러 처리 
    ,error=function(e) {
    print(paste0("#### ERROR : ",looping_ar[i],"  ####"))
    break()
    }
)
print('Reading tables done~!')
    
# stack to DF
DM_PL_PLC <- rbindlist(DM_PL_PLC_LIST)
DM_PL_PTY <- rbindlist(DM_PL_PTY_LIST)
    
# rename the features in DM_PL_PTY from ??? to CST_???
Merge_Keys < which(colnames(DM_PL_PTY) %in% c("BAS_YM", "PLHD_INTG_CUST_NO"))
colnames(DM_PL_PTY)[-Merge_Keys] <- paste0("CST_",colnames(DM_PL_PTY)[-Merge_Keys])
                                                                                                 
# merge
setkey(DM_PL_PLC,BAS_YM,PLHD_INTG_CUST_NO)
setkey(DM_PL_PTY, BAS_YM, PLHD_INTG_CUST_NO ) 
dataXY <- DM_PL_PTY[DM_PL_PLC]

# save memory 
rm(DM_PL_PLC) 
rm(DM_PL_PTY) 
rm(DM_PL_PLC_LIST) 
rm(DM_PL_PTY_LIST) 
rm(DM_PL_PLC.org) 
rm(DM_PL_PTY.org)

# print Log
cat("dataXY returned!","\n")
gc(reset - T)