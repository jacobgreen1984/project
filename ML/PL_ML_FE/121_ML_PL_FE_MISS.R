# ---
# title   : 121_ML_PL_FE_MISS
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 결측값 정의
# ---


#	replace NA features With 9999
NA_FEATURE <- grep("ELPS_DAY", colnames(dataXY), value=T)
  
for(i in NA_FEATURE){
  set(dataXY, which(is.na(dataXY[[i]])), i, 9999)
}


# replace NA features with 0 
NA_FEATURE <- c("MNCV_CMIP",
                "RID_CMIP")

for(i in NA_FEATURE){
  set(dataXY, which(is.na(dataXY[[i]])), i, 0)
}
                            

# replace NA features with mode 
NA_FEATURE <- c("TRSF_HOPE_DAY",
                "MNCV_PYMT_PRD_TYP_CD",
                "PLHD_HOME_ADDR_LOCR_CD",
                "MINSD_HOME_ADDR_LOCR_CD", 
                "PLHD_IJY_RSKLVL_CD",
                "PLHD_FC_GEN_CD",
                "PLHD_GEN_CD",
                "MINSD_IJY_RSKLVL_CD",
                "MINSD_OCPN_RSKLVL_CD",
                "FC_GEN_CD",
                "MINSD_GEN_CD")
                            
for(i in NA_FEATURE){	
   set(dataXY, which(is.na(dataXY[[i]])), i, getmode(dataXY[[i]]))
}


# replace NA features with N 
NA_FEATURE <- c("PLHD_EMAIL_RCVE_CNST_FLG", 
                "PLHD_DSOD_FLG",
                "MINDS_DSOD_FLG")

for(i in NA_FEATURE){	
   set(dataXY, which(is.na(dataXY[[i]])), i, "N")
}


# replace NA features with median
NA_FEATURE <- c("PLHD_CUR_AGE", 
                "PLHD_PLC_AGE",
                "PLHD_MINSD_CUR_AGE_DIFF")
 
for(i in NA_FEATURE){	
  set(dataXY, which(is.na(dataXY[[i]])), i, median(dataXY[[i]], na.rm=T))
}


# replace missing values of numeric features
NUHERIC.FEATURE <- which(sapply(dataXY, is.numeric))
                                        
for(i in NUMERIC_FEATURE){
   set(dataXY, which(is.na(dataXY[[i]])), i, 0)
   set(dataXY, which(ls.infinite(dataXY[[i]])), i, 0)
}


# print Log
cat(">> REPLACE MISSING VALUES done!", "\n") 
gc(reset-T)