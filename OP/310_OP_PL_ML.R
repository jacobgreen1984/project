# ---
# title   : 310_OP_PL_ML
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 운영 베스트모델 모드 및 예측데이터셋 생성
# ---


#	set HDFS PATH
BASE_ML_HDFS_Path       <- file.path(HDFS_Path, "/data/MDEL/EXPT_MDEL_PL_FRCST/BASE_ML")
BEST_ML_HDFS_Path       <- file.path(HDFS_Path, "/data/MDEL/EXPT_MDEL_PL_FRCST/BEST_ML")
MDEL_EVAL_RST_HDFS_Path <- file.path(HDFS_Path, "/data/MDEL/EXPT_MDEL_PL_FRCST/MDEL_EVAL_RST")

# convert to h2o
test <- as.h2o(dataXY)

# Load BEST ML
BEST_ML <- H2o.loadModel(file.path(HDFS_Path, hdfs.ls(BEST_ML_HDFS_Path)$file))

# Load REPORT
ML_PATH <- gsub(".*\\BASE_ML/", "", hdfs.ls(BASE_ML_HDFS_Path)$file)
BASE_LEARNER_MAX_AUC <- as.numeric(max(gsub(".*\\_", "", ML_PATH)))
STACK_MODEL_REPORT	 <- h2o.importFile(path=file.path(MDEL_EVAL_RST_HDFS_Path, "PL_ML_STACK_MODEL_REPORT.csv"))
STACK_MODEL_MAX_AUX  <- max(STACK_MODEL_REPORT$AUC)

# make test dataset
if(BASE_LEARNER_MAX_AUC < STACK_MODEL_MAX_AUX){
  
  # Load BASE LEARNERS
  ML_PATH = gsub(".*\\BASE_ML/", "", hdfs.ls(BASE_ML_HDFS_Path)$file)
  ML_NAME = gsub("_.*", "", ML_PATH)
  BASE_LEARNERS <- list()
    for(i in 1:length(ML_PATH)){
      BASE_LEARNERS[[ML_NAME[i]]] <- h2o.loadModel(file.path(HDFS_Path, hdfs.ls(file.path(HDFS_Path, hdfs.ls(BASE_ML_HDFS_Path)$file))$file)[i])
      cat(BASE_LEARNERS[[ML_NAME[i]]]@model_id, "loaded!", "\n") 
    }
    
    # convert to type
    tryCatch(test$PL_FLG <- h2o.asfactor(test$PL_FLG), error=function(e) cat(""))
    
    # make test for stacking
    ML_NAME = gsub("_.*", "", ML_PATH)
    TEST_PROB = list()
    for(i in 1:length(ML_PATH)){
      TEST_PROB[[ML_NAME[i]]] <- setNames(predict(BASE_LEARNERS[[ML_NAME[i]]], test)[,3], ML_NAME[i])
    }
    
    # Load INTEVAL
    STACK_MODEL_REPORT <- as.data.table(h2o.importFile(path=file.path(MDEL_EVAL_RST_HDFS_Path, "PL_ML_STACK_MODEL_REPORT.csv")))
    INTERVAL <- STACK_MODEL_REPORT[AUC==max(AUC), INTERVAL][1]
    
    # assign the best base Learner
    BEST_BL = which.max(gsub(".*\\_", "", ML_PATH))
    
    # make stacking dataset
    if(INTERVAL!=0.5){
      #with tag
      TEST_FOR_STACK     <- h2o.cbind(test$PL_FLG, Reduce(h2o.cbind, TEST_PROB))
      TEST_FOR_STACK$TAG <- h2o.asfactor(h2o.ifelse(TEST_PROB[[BEST_BL]] >= 0.5-INTERVAL & TEST_PROB[[BEST_BL]] <= 0.5+INTERVAL, 1, 0))
    } else{
      # without tag
      cat(">> if INTERVAL is", INTERVAL, "do not make TAG! <<", "\n")
      TEST_FOR_STACK <- h2o.cbind(test)
    }
    
    # make NEW test for stacking
    test <- h2o.cbind(test[,c("BAS_YM", "PLHD_INTG_CUST_NO", "PONO", "TOT_PL_TIMES_MM11.CUMSUM")], TEST_FOR_STACK)
    
    # print Log
    cat(">> BASE_LEARNER is ready to use", "\n") 
    
}else{
  # print Log
  cat(">> BASE_LEARNER is ready to use", "\n") 
}

# reset
gc(reset=T) 

