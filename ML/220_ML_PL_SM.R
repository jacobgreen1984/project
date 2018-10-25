# ---
# title   : 220_NL_PL_SM
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 모형앙상블
# ---


# Laod BASE LEARNERS
ML_PATH = list.files(path=file.path(EXPT_PL_Path, "BASE_ML"))
ML_NAME = gsub("_.*","",ML_PATH)
BASE_LEARNERS <- list()
for(i in 1:length(ML_PATH)){
  BASE_LEARNERS[[ML_NAME[i]]] <- h2o.loadMode1(file.path(EXPT_PL_Path,"BASE_ML", ML_PATH[i],list.files(file.path(EXPT_PL_Path,"BASE_ML",ML_PATH[i]))))
  cat(BASE_LEARNERS[[ML_NAME[i]]]@model_id,"loaded!","\n")
}

# update BASE_LEARNERS's AUC on the recent test datatset
ML_PATH = list.files(path=file.path(EXPT_PL_Path, "BASE_ML"))
ML_NAME = gsub("_.*","",ML_PATH)
for(i in 1:length(ML_PATH)){
  OLD_NAME = file.path(EXPT_PL_Path,"BASE_ML", ML_PATH[i])
  NEW_NAME = file.path(EXPT_PL_Path,"BASE_ML",paste0(ML_NAME[i],"_",round(h2o.auc(h2o.performance(BASE_LEARNERS[[ML_NAME[i]]],newdata=test)),4)))
  if(OLD_NAME!=NEW_NAME){
     system(paste("mv ",OLD_NAME,NEW_NAME))
     cat(BASE_LEARNERS[[ML_NAME[i]]]@model_id,"'s AUC updated!","\n")
  }else cat(ML_NAME[i],">> OLD_NAME is equal to NEW_NAME!","\n")
}

# re-Laod BASE_LEARNERS
ML_PATH = list.files(path = file.path(EXPT_PL_Path, "BASE_ML"))
ML_NAME = gsub("_.*","",ML_PATH)
BASE_LEARNERS <- list()
for(i in l:length(ML_PATH)){
  BASE_LEARNERS[[ML_NAME[i]]] <- h2o.loadModel(file.path(EXPT_PL_Path,"BASE_ML", ML_PATH[i], list.files(file.path(EXPT_PL_Path,"BASE_ML",ML_PATH[i]))))
  cat(BASE_LEARNERS[[ML_NAME[i]]]@model_id,"re-loaded!","\n")
}

# make valid and test for stacking 
ML_NAME = gsub("_.*","",ML_PATH)
VALID_PROB = list()
TEST_PROB = list()
for(i in 1:length(ML_PATH)){
  VALID_PROB[[ML_NAME[i]]] <- setNames(predict(BASE_LEARNERS[[ML_NAME[i]]],valid)[,3],ML_NAME[i])
  TEST_PROB[[ML_NAME[i]]]  <- setNames(predict(BASE_LEARNERS[[ML_NAME[i]]],test)[,3],ML_NAME[i])
}                                                                                             

# setting interval search option
if(isTRUE(SEARCH_INTERVAL)){
  INTERVAL = c(seq(from=0.05, to=0.5, by=0.1),0.5)
}else{
  INTERVAL = BEST_INTERVAL
  cat("INTERVAL: ",INTERVAL,"\n")
}

#trail. and error test for stacking model
STACK_MODEL	=	list()
STACK_MODEL_REPORT = list()
for(i in 1:length(INTERVAL)){
    # create DATA_FOR_STACK
    DATA_FOR_STACK <- makeDataForStack(valid,test,INTERVAL=INTERVAL[i])
    
    #run Stacking Model
    STACK_MODEL[[i]]	<- h2o.automl(
      training_frame = DATA_FOR_STACK$train, 
      x = which(colnames(DATA_FOR_STACK$train) != "PL_FLG"),
      y = which(colnames(DATA_FOR_STACK$train) == "PL_FLG"),
      seed=1234,
      
      #	stoping options
      max_runtime_secs = max_runtime_secs, 
      max_models = max_models
    )
   
    #	make report
    STACK_MODEL_REPORT[[i]] <- data.frame(
      INTERVAL = INTERVAL[i],
      AUC = h2o.auc(h2o.performance(STACK_MODEL[[i]]@leader,newdata=DATA_FOR_STACK$test))
    )
    
    cat("INTERVAL: ",INTERVAL[i],"\n")
    cat("AUC: ",STACK_MODEL_REPORT[[i]]$AUC,"\n")
}

# make STACK_MODEL_REPORT
STACK_MODEL_REPORT <- Reduce(rbind,STACK_MODEL_REPORT)
BestML_FROM_STACK  <- list(
  ML = STACK_MODEL[which.max(STACK_MODEL_REPORT$AUC)][[1]]@leader,
  INTERVAL=STACK_MODEL_REPORT$INTERVAL[which.max(STACK_MODEL_REPORT$AUC)]
)

# save REPORT
 write.csv(STACK_MODEL_REPORT, file.path(EXPT_PL_Path,"REPORT","PL_ML_STACK_MODEL_REPORT.csv"))

#	print Log
cat("STACK_MODEL_REPORT returned!","\n")
cat(">> BestML From BASE LEARNERS: ",max(gsub(".*\\_","",ML_PATH)),"\n")
cat(">> BestML From STACKING BASE: ",round(max(STACK_MODEL_REPORT$AUC),4),"\n")
gc(reset=T)
                                        