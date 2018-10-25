# ---
# title   : 220_OP_PL_FE
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 운영 데이터전처리 및 파생변수 생성
# ---


#	remove unwanted features
dataXY[,(which(colnames(dataXY) %in% c("CMPL_WVPRM_FLG",
                                       "INSD_SF_FLG",
                                       "MINSD_INTG_CUST_NO",
                                       "PLHD_OFFC_ADDR_LOCL_CD",
                                       "SLFC_NO",
                                       "FC_CUR_AGE",
                                       "PLHD_FC_CUR_AGE_DIFF",
                                       "PLHD_JBKND_CD",
                                       "PLHD_JBKND_L1_CD",
                                       "MINSD_JBKND_CD",
                                       "MINSD_JBKND_L1_CD",
                                       "SVFC_NO"))):=NULL]

# replace missing value CODE(99, ZZ) with NA
dataXY[PLHD_MINSD_CUR_AGE_DIFF==999, PLHD_MINSD_CUR_AGE_DIFF:=NA]    # 999: 불명
dataXY[PLHD_PLC_AGE==999, PLHD_PLC_AGE:=NA]                          # 999: 불명
dataXY[MNCV_PYMT_PRD_TYP_CD=="99", MNCV_PYMT_PRD_TYP_CD:=NA]         # 99: 불명 
dataXY[PMFQY_CD=="ZZ", PMFQY_CD:=NA]                                 # ZZ: 불명, 99: 일시납
dataXY[PLHD_GEN_CD=="ZZ", PLHD_GEN_CD:=NA]                           # ZZ: 불명
dataXY[PLHD_HOME_ADDR_LOCL_CD=="ZZ", PLHD_HOME_ADDR_LOCL_CD:=NA]     # ZZ: 불명
dataXY[PLHD_IJY_RSKLVL_CD=="ZZ", PLHD_IJY_RSKLVL_CD:=NA]             # ZZ: 불명
dataXY[PLHD_DSOD_FLG=="ZZ", PLHD_DSOD_FLG:=NA]                       # ZZ: 불명
dataXY[PLHD_EMAIL_RCVE_CNST_FLG=="Z", PLHD_EMAIL_RCVE_CNST_FLG:=NA]  # Z: 불명
dataXY[MINSD_GEN_CD=="ZZ", MINSD_GEN_CD:=NA]                         # ZZ: 불명
dataXY[MINDS_OCPN_RSKLVL_CD=="ZZ", MINDS_OCPN_RSKLVL_CD:=NA]         # ZZ: 불명
dataXY[MINDS_IJY_RSKLVL_CD=="ZZ", MINDS_IJY_RSKLVL_CD:=NA]           # ZZ: 불명
dataXY[MINSD_HOME_ADDR_LOCL_CD=="ZZ", MINSD_HOME_ADDR_LOCL_CD:=NA]   # ZZ: 불명
dataXY[MINSD_DSOD_FLG=="Z", MINSD_DSOD_FLG:=NA]                      # Z: 불명
dataXY[PMPMD_CD=="ZZ", PMPMD_CD:=NA]                                 # ZZ: 불명, 99: 해당없음
dataXY[KLIA_INS_KND_CD=="ZZ", KLIA_INS_KND_CD:=NA]                   # ZZ: 불명, 99: 기타보험
dataXY[TRSF_HOPE_DAY=="ZZ", TRSF_HOPE_DAY:=NA]                       # ZZ: 불명
dataXY[PROD_REPT_TYP_CD=="ZZ", PROD_REPT_TYP_CD:=NA]                 # ZZZ: 불명
dataXY[MNCV_INS_PRD_TYP_CD=="ZZ", MNCV_INS_PRD_TYP_CD:=NA]           # ZZ: 불명, 99: 종신형
dataXY[FC_GRD_CD=="0", FC_GRD_CD:=NA]                                # 0: 불명
dataXY[FC_GEN_CD=="ZZ", FC_GEN_CD:=NA]                               # ZZ: 불명

# make additional features
dataXY[,NML_INTRT:=ifelse(is.na(NML_INTRT), median(NML_INTRT, na.rm=T), NML_INTRT), by=list(BASE_YM, MKTG_PROD_GRP_L4_CD)]
dataXY[,NML_INTRT:=ifelse(is.na(NML_INTRT), median(NML_INTRT, na.rm=T), NML_INTRT), by=list(MKTG_PROD_GRP_L4_CD)]
dataXY[,NML_INTRT:=ifelse(is.na(NML_INTRT), median(NML_INTRT, na.rm=T), NML_INTRT)]                                         # 대출 이자율
dataXY[,CST_LST_LOAN_ELPS_DAYS := pmin(CST_LST_NEW_PL_ELPS_DAYS, CST_LST_ADTL_PL_ELPS_DAYS, na.rm=T)]                       # 고객 최종 대출 경과일수
dataXY[,TOT_PL_TIMES_BAS_YM := sum(TOT_PL_TIMES_MM00, na.rm=T), by=BASE_YM]                                                 # 총 당월 대출건수
dataXY[,TOT_PL_TIMES_BAS_YM_L1 := sum(TOT_PL_TIMES_MM01, na.rm=T), by=BASE_YM]                                              # 총 전월 대출건수
dataXY[,TOT_PL_TIMES_BAS_YM_INCRT := (TOT_PL_TIMES_BAS_YM - TOT_PL_TIMES_BAS_YM_L1)/TOT_PL_TIMES_BAS_YM_L1]                 # 전월 대비 당월 대출건수 증감률
dataXY[,BAS_M := as.factor(substring(dataXY[, BAS_YM], 5, 6))]                                                              # 기준월
dataXY[,PLHD_CUR_AGE_CD := cut(PLHD_CUR_AGE, breaks=seq(from=0, to=100, 5))]                                                # 계약자 현재연령 구간
dataXY[,TOT_PL_TIMES_BAS_YM_L1 := NULL]

# replace missing valueS
source(file.path(ML_PL_PATH, "ML_PL_FE", "121_ML_PL_FE_MISS.R"))

# make cumulative features
source(file.path(ML_PL_PATH, "ML_PL_FE", "122_ML_PL_FE_CUMU.R"))

# make customer features
source(file.path(ML_PL_PATH, "ML_PL_FE", "123_ML_PL_FE_CUST.R"))

# convert from char to factor
CHARACTER_FEATURE = names(Filter(is.character, dataXY))
dataXY[,(CHARACTER_FEATURE):=lapply(.SD, function(x) as.factor(x)), .SDcols=CHARACTER_FEATURE]
cat(">> all character_features converted to factor_features!", "\n") 

# round NUMERIC FEATURE
if(ifTRUE(doRound)){
  NUMERIC_FEATURE = colnames(dataXY)[grepl("PL_PSB_AMT|PL_BLC|CMIP|_AMT_|_WAMT_", colnames(dataXY))]
  dataXY[,(NUMERIC_FEATURE):=lapply(.SD, function(x) signif(x, digits=2)), .SDcols=NUMERIC_FEATURE]
  cat(">> ROUND FUNCTION applied!", "\n")
} else cat(">> ROUND FUNCTION not applied!", "\n")

# make ratio features
dataXY[,PL_BLC_PSB_AMT_RATE := PL_BLC/(PL_BLC + PL_PSB_AMT)]                                                     # 계약 대출잔고/(대출잔고+대출가능금액)
dataXY[,CST_PL_BLC_PSB_AMT_RATE := CST_PL_BLC.SUM/(CST_PL_BLC.SUM + CST_PL_PSB_AMT.SUM)]                         # 고객 대출잔고/(대출잔고+대출가능금액)                  
dataXY[,PL_BLC_PAID_PRM_RATE := PL_BLC/TOT_PAID_PRM]                                                             # 계약 대출잔고/총납입보험료
dataXY[,CST_PL_BLC_PAID_PRM_RATE := CST_PL_BLC.SUM/CST_TOT_PAID_PRM.SUM]                                         # 고객 대출잔고/총납입보험료
dataXY[,SRDVLU_WAMT_PAID_PRM_RATE := (TOT_PAID_PRM_L1-SRDVLU_WAMT)/TOT_PAID_PRM_L1]                              # 계약 (총납입보험료-해약환급금)/총납입보험료
dataXY[,CST_SRDVLU_WAMT_PAID_PRM_RATE := (CST_TOT_PAID_PRM_L1.SUM-CST_SRDVLU_WAMT.SUM)/CST_TOT_PAID_PRM_L1.SUM]  # 고객 (총납입보험료-해약환급금)/총납입보험료
dataXY[,PL_PSB_AMT_PAID_PRM_RATE := PL_PSB_AMT/TOT_PAID_PRM_L1]                                                  # 계약 대출가능금액/총납입보험료
dataXY[,CST_PL_PSB_AMT_PAID_PRM_RATE := CST_PL_PSB_AMT.SUM/CST_TOT_PAID_PRM_L1.SUM]                              # 고객 대출가능금액/총납입보험료
dataXY[,CST_TOT_PAID_PRM_L1.SUM := NULL]
dataXY[,CST_TOT_PAID_PRM_L1.MAX := NULL]

# replace missing values from additional features
Num_Features = which(sapply(dataXY, is.numeric))
for(i in Num_Features){
  set(dataXY, which(is.na(dataXY[[i]])), i, 0)
  set(dataXY, which(is.infinite(dataXY[[i]])), i, 0)
}

# print Log
cat(">> dataXY is returned!", "\n") 
gc(reset=T) 