# ---
# title   : 310_ML_PL_BST
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 베스트모델 선택 및 저장
# ---


# save BEST_ML
BestAUC_BASE = as.numeric(max(gsub(".*\\_","",ML_PATH)))
BestAUC_STACK = round(max(STACK_MODEL_REPORT$AUC),4)

if(BestAUC_BASE>=BestAUC_STACK){

  #	remove ail files in BEST_ML
  system("rm -rf /data/EXPT_MDEL/MDEL_PL_FRCST/BEST_ML/*")
  
  #	save the BEST_ML in BEST_ML folder
  BEST_ML <- BASE_LEARNERS[which.max(gsub(".*\\_","",ML_PATH))][[1]] 
  h2o.saveModel(BEST_ML,file.path(EXPT_PL_Path, "BEST_ML"),force=T)
  cat("Base_Learner selected for the Best ML","\n")
           
}else{
    
    #remove all files in BEST_ML
    system("rm -rf /data/EXPT_MDEL/MDEL_PL_FRCST/BEST_ML/*")
    
    # save the BEST_ML in BEST_ML folder
    BEST_ML <- BestML_FROM_STACK[[1]]
    h2o.saveModel(BEST_ML, file.path(EXPT_PL_PathEXPT_PL_Path, "BEST_ML"),force=T)
    cat("Stacking_Model selected for the Best ML","\n")
      
    # convert dataset for stacking
    DATA_FOR_STACK <- makeDataForStack(valid,test,INTERVAL=BestML_FROM_STACK[[2]])
    test           <-	h2o.cbind(DATA_FOR_STACK$test,test[,c("PLHD_INTG_CUST_NO","TOT_PL_TIMES_MM11.CUMSUM","PONO","BAS_YM")])

}
    
# print Log
cat("BEST_ML saved!","\n")
gc(reset=T)
      