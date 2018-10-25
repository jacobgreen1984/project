# ---
# title   : OP_PL_Rcode 
# version : 0.1
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 운영 코드
# ---

# 메모리 초기화 
rm(list = ls())                                                   
gc(reset = T)                                                        

# Configure 호출 
scriptPath <- "/data/rpjt/R_script/MDEL_PL_FRCST"                                  
source(file.path(scriptPath, "PL_CONF.R"))                              

# H2O세션 열기
library(h2o)                                                        
h2o.init(nthreads=24)                                                 

# SparkR 세션 열기
setwd("~")                                                               
sparkR.session(master = SPARK_Path)                                    

# Sys.sleep for network communication
Sys.sleep(5)                                                            

# 배치변수 input
args     <- commandArgs(trailingOnly = TRUE)
yyyymm   <- args[1]
term     <- args[2]

# yyyymm null 처리
if(is.null(yyyymm) | is.na(yyyymm)){
  readTablesFromSpark(S_EDW_Path, "CO_STDATE_MGMT")
  df <- SparkR::sql("SELECT YM FROM CO_STDATE_MGMT WHERE STDATE_TYP_CD = '01'")
  yyyymm <- as.data.frame[1,]
}

# yyyymm, term 입력값 출력
if(is.null(term) | is.na(term)){
  term <- 1
}

# yyyymm, term 입력값 출력 
cat(">> input_parameters are : ","yyyymm = ",yyyymm,"& term = ",term,"\n")

# 기간 설정 
looping_ar <- makeDateArray('201709', 1)
cat(">> looping_ar is : ", looping_ar,"\n")

# 권한부여 설정 
chmod_777 <- TRUE                                                       

# 계약 템프테이블
source(file.path(OP_PL_Path,"110_OP_PL_PLC_TT.R"))                      

# 당사자 템프테이블
source(file.path(OP_PL_Path,"120_OP_PL_PTY_TT.R"))                      

# DM 마트 이관
source(file.path(OP_PL_Path,"130_OP_PL_MD.R"))                          

# 데이터셋 생성 
source(file.path(OP_PL_Path,"210_OP_PL_DT.R"))                          

# 파생변수 생성
doRound = T                                                             
source(file.path(OP_PL_Path,"220_OP_PL_FE.R"))                            

# BEST모델 예측
source(file.path(OP_PL_Path,"310_OP_PL_ML.R"))                          

# BEST모델 운영 output 생성 
source(file.path(OP_PL_Path,"411_OP_PL_FRCST_RST.R"))                    
