#---
# title   : 210_ML_PL_BL
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 변수선택 및 베이스러너 생성
#---

# choose features
PLC <- colnames(dataXY)[!grepl(".MM[0-9][1-9]$|.MM10$|CST_|CUMSUM",colnames(dataXY))]
CST <- grep("CST_",colnames(dataXY)[!grepl(".MM[0-9][1-9]$|.MM10$|CUMSUM",colnames(dataXY))],value=T)
CM01 <- grep("MM01.CUMSUM",colnames(dataXY),value=T)
CM02 <- grep("MM02.CUMSUM" ,colnames(dataXY),value=T)
CM11 <- grep("MM11.CUMSUM",colnames(dataXY),value=T)

# convert to h2o.object
dataXY.h2o <- as.h2o(dataXY[,c(PLC,CST,CM01,CM02,CM11),with=F])
dataXY.h2o$PL_FLG <- h2o.asfactor(dataXY.h2o$PL_FLG)

# split dataset
splits	<-	h2o.splitFrame(dataXY.h2o,ratios= c(0.6,0.2),seed= 1234)
train 	<-	splits[[1]]
valid	  <-	splits[[2]]
test	  <-	splits[[3]]

# set X and Y
x <- which(!colnames(train) %in% c("PL_FLG","PONO","BAS_YM","PLHD_INTG_CUST_NO")) 
y <- which(colnames(train) == "PL_FLG")

# find Best_Features
source(file.path(ML_PL_Path,"ML_PL_BL","211_ML_PL_BL_BF.R"))

# set X and Y with respect to best_feature_report
x <- VI_FROM_XGB[1:BEST_FEATURE_REPORT[AUC==max(AUC),NUM_FEATURE]]
y <- "PL_FLG"

# RF
if(isTRUE(RF_DEFAULT_TRAIN)){
  ML_RF = h2o.randomForest(x=x, y=y, training_frame=train, seed=1234)
}else{
  source(file.path(ML_PL_Path,"ML_PL_BL","212_ML_PL_BL_RF.R"))
  ML_RF = h2o.getModel(sortedGrid@model_ids[[1]])
}
ML_RF_AUC = h2o.auc(h2o.performance(ML_RF,newdata=test))
cat("AUC(RF): ",ML_RF_AUC,"\n")

# GBM
if(lsTRUE(GBM_DEFAULT_TRAIN)){
  ML_GBM = h2o.gbm(x=x, y=y, training_frame=train, seed=1234)
}else{
  source(file.path(ML_PL_Path,"ML_PL_BL","213_ML_PL_BL_GBM.R"))
  ML_GBM = h2o.getModel(sortedGrid@model_ids[[1]])
}
ML_GBM_AUC - h2o.auc(h2o.performance(ML_GBM,newdata=test))
cat("AUC(GBM): ",ML_GBM_AUC,"\n")

# DL
if(isTRUE(DL_DEFAULT_TRAIN)){
  ML_DL = h2o.deeplearning(x=x, y=y, training_frame=train, standardize=T,seed=1234)
}else{
  source(file.path(ML_PL_Path,"ML_PL_BL","214_ML_PL_BL_DL.R"))
  ML_DL = h2o.getModel(sortedGrid@model_ids[[1]])
}
ML_DL_AUC = h2o.auc(h2o.performance(ML_DL,newdata=test))
cat("AUC(DL):	",ML_DL_AUC,"\n")

# XGB
if(isTRUE(XGB_DEFAULT_TRAIN)){
  ML_XGB = h2o.xgboost(x=x, y=y, training_frame=train, categorical_encoding ="EnumLimited",seed=1234)
}else{
  source(file.path(ML_PL_Path,"ML_PL_BL","215_ML_PL_BL_XGB.R"))
  ML_XGB = h2o.getModel(sortedGrid@model_ids[[1]])
}
  ML_XGB_AUC = h2o.auc(h2o.performance(ML_XGB,newdata=test))
  cat("AUC(XGB): ", ML_XGB_AUC,"\n")
  
# AML
if(lsTRUE(AML_DEFALILT_TRAIN)){
  ML_AML = h2o.automl(x=x, y=y, training_frame=train, seed=1234, max_runtime_secs=60*10, max_models=10)@leader
}else{
  source(file.path(ML_PL_Path,"ML_PL_BL","216_ML_PL_BL_AML.R"))
  ML_AML = ML_AML@leader
}
ML_AML_AUC = h2o.auc(h2o.performance(ML_AML,newdata=test))
cat("AUC(AML): ",ML_AML_AUC, "\n")
  
# save BASELEARNERs
CURRENT_DATE = gsub("-","",Sys.Date())
h2o.saveModel(ML_RF, file.path(EXPT_PL_Path,"BASE_ML",paste0(CURRENT_DATE,"RF_" ,round(ML_RF_AUC,4))) ,force=T)
h2o.saveModel(ML_GBM,file.path(EXPT_PL_Path,"BASE_ML",paste0(CURRENT_DATE,"GBM_",round(ML_GBM_AUC,4))),force=T) 
h2o.saveModel(ML_DL, flle.path(EXPT_PL_Path,"BASE_ML",paste0(CURRENT_DATE,"DL_" ,round(ML_DL_AUC,4))) ,force=T) 
h2o.saveHodel(ML_XGB,flie.path(EXPT_PL_Path,"BASE_ML",paste0(CURRENT_DATE,"XGB_",round(ML_XGB_AUC,4))),force=T) 
h2o.saveModel(ML_AML,flle.path(EXPT_PL_Path,"BASE_ML",paste0(CURRENT_DATE,"AML_",round(ML_AML_AUC,4))),force=T)

# BASE_LEARNER_REPORT
BASE_LEARNER_REPORT <- data.frame(
  BASE_LEARNER = c("RF","GBM","DL","XGB","AML"),
  AUC = c(ML_RF_AUC,ML_GBM_AUC,ML_DL_AUC,ML_XGB_AUC,ML_AML_AUC)
)
   
# save REPORT
write.csv(BASE_LEARNER_REPORT,file.path(EXPT_PL_Path, "REPORT", "PL_ML_BASE_LEARNER_REPORT.csv"))
   
# print Log
print(BASE_LEARNER_REPORT[order(BASE_LEARNER_REPORT$AUC, decreasing=T),])
cat("BASE_LEARNER_REPORT returned!","\n") 
gc(reset=T)