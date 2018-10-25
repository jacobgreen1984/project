# ---
# title   : ML_PL_Rcode 
# version : 0.1
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 머신러닝 코드
# ---

# 메모리 초기화 
rm(list = ls())                                                         # Remove all object
gc(reset = T)                                                           # Release memory

# Configure 호출 
scriptPath <- "/data/rpjt/R_script/MDEL_PL_FRCST"                       # Set script path             
source(file.path(scriptPath, "PL_CONF.R"))                              # Load Configures

# sparklyR 세션 열기
setwd("~")                                                              # set working directory(avoid the hive metastore_db problem) 
sparklyR.conn("cluster",24)                                             # Cluster Mode

# 기간 설정 
looping_ar <- makeDateArray('201705', 24)                               # 기준년월(M시점, 예측 타겟은 M+1, M_2 시점)

# 데이터셋 생성 
source(file.path(ML_PL_Path,"110_ML_PL_DT.R"))                          # 계약 및 고객 데이터를 결합하여 분석 데이터셋 생성 

# sparklyR 세션 끊기
sparklyR.disconn(sc)                                                    # Disconnect sparklyR

# 파생변수 생성
doRound = T                                                             # 연속형 변수 라운드 처리 옵션 
doSample = 0.1                                                          # 전체 데이터셋에서 샘플링 진행
source(file.path(ML_PL_Path,"120_ML_PL_FE.R"))                          # R data.table을 사용하여 통계 및 비율 파생변수 생성 

# H2O세션 열기
library(h2o)                                                            # H2O 라이브러리 호출
h2o.init(nthreads=24)                                                   # H2O 연결(local)

# 모델학습 옵션 설정
RF_DEFAULT_TRAIN  = F                                                   # 빠른학습 설정
GBM_DEFAULT_TRAIN = F                                                   # 빠른학습 설정
DL_DEFAULT_TRAIN  = F                                                   # 빠른학습 설정
XGB_DEFAULT_TRAIN = F                                                   # 빠른학습 설정
AML_DEFAULT_TRAIN = F                                                   # 빠른학습 설정

# 기초모델 생성
max_models_AML = 30                                                     # 모형튜닝 과정에서 최대 모형 개수 설정
max_models_RF  = 10                                                     # 모형튜닝 과정에서 최대 모형 개수 설정
max_models_XGB = 100                                                    # 모형튜닝 과정에서 최대 모형 개수 설정
max_models_GBM = 100                                                    # 모형튜닝 과정에서 최대 모형 개수 설정
max_models_DL  = 30                                                     # 모형튜닝 과정에서 최대 모형 개수 설정
max_runtime_secs = 0                                                    # 모형튜닝 과정에서  최대학습소요시간 설정
source(file.path(ML_PL_Path,"210_ML_PL_BL.R"))                          # 1차) 베스트변수 선택, 2차) 기초모델 생성

# 기초모델 백업
CURNT_DATE = file.path(EXPT_PL_Path,"bak",gsub("-","",Sys.Date()))      # 백업 경로 : /data/EXPT_MDEL/MDEL_PL_FRCST/bak
system(paste("mkdir",CURNT_DATE))                                       # 백업폴더 생성
system(paste("cp -r",paste0(EXPT_PL_Path,"/BASE_ML/*"),CURNT_DATE))     # 기초모델 백업

# XGB모델 이동 
system(paste("mv -b",paste0(EXPT_PL_Path,"/BASE_ML/*XGB*"),CURNT_DATE)) # H2O 3.14.0.2이하 버전에서는 XGB를 BASE_ML에서 제거

# 앙상블모델 생성
max_models= 30                                                          # 모형튜닝 과정에서 최대모형개수 설정 
max_runtime_secs = 60*60*10                                             # 모형튜닝 과정에서 최대학습소요시간 설정
SEARCH_INTERVAL = T                                                     # 태깅간격 탐색 옵션 
BEST_INTERVAL = 0.35                                                    # 예측확률 50%를 기준점으로 태깅 간격 베스트값 설정
source(file.path(ML_PL_Path,"220_ML_PL_SM.R"))                          # /data/EXPT_MDEL/MDEL_PL_FRCST/BASE_ML 안에 존재하는 모델 앙상블

# BEST모델 저장
source(file.path(ML_PL_Path,"310_ML_PL_BST.R"))                         # 기초모델과 앙상블모델을 비교하여 BEST모델 설정 및 저장

# BEST모델 리포트 생성
source(file.path(ML_PL_Path,"320_ML_PL_RST.R"))                         # BEST모델 리포트 생성

# H2O세션 종료
h2o.shutdown(prompt=F)                                                  # H2O 세션을 종료 
gc(reset=T)                                                             # 메모리 초기화