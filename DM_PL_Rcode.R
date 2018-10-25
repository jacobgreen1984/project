# ---
# title   : DM_PL_Rcode 
# version : 0.1
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 데이터마트 코드
# ---

# 메모리 초기화 
rm(list = ls())                                                         # Remove all object
gc(reset = T)                                                           # Release memory

# Configure 호출 
scriptPath <- "/data/rpjt/R_script/MDEL_PL_FRCST"                       # Set script path             
source(file.path(scriptPath, "PL_CONF.R"))                              # Load Configures

# 권한부여 설정 
chmod_777 <- TRUE                                                       # 생성 파일에 권한 777부여 여부 설정 

# 기간 설정 
looping_ar <- makeDateArray('201705', 24)                               # 기준년월(M시점, 예측 타겟은 M+1, M_2 시점)

# SparkR 세션 열기
setwd("~")                                                              # set working directory(avoid the hive metastore_db problem) 
sparkR.session(master = SPARK_Path)                                     # Spark Standalone Mode

# 계약 템프테이블
source(file.path(DM_PL_Path,"110_DM_PL_PLC_TT.R"))                      # WRK 템프테이블 생성(1)

# 당사자 템프테이블
source(file.path(DM_PL_Path,"120_DM_PL_PTY_TT.R"))                      # WRK 템프테이블 생성(2)

# DM 마트 이관
source(file.path(DM_PL_Path,"210_DM_PL_MD.R"))                          # DM 마트 이관

# SparkR 세션 끊기 
sparkR.session.stop()                                                   # Disconnect SparkR

# 메모리 초기화 
rm(list=ls())                                                           # Remove all object
gc(reset=T)                                                             # Release memory