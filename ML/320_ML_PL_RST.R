# ---
# title   : 320_ML_PL_RST
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 리포트 생성 
# ---


# make KS
KS = makeKS(ml=BEST_ML, newdata=test)
write.csv(KS, file.path(EXPT_PL_Path, "REPORT", "PL_ML_KS.csv"))

#	make LIFT
LIFT = makeLIFT(ml=BEST_ML, newdata=test)
write.csv(LIFT, file.path(EXPT_PL_Path, "REPORT", "PL_ML_LIFT.csv"))

# make Confusion_matrix
PLC_CM = makeCONFUSIONMATRIX(ml=BEST_ML, newdata=test, type="PLC")
CST_CM = makeCONFUSIONMATRIX(ml=BEST_ML, newdata=test, type="CST")
write.csv(PLC_CM, file.path(EXPT_PL_Path, "REPORT", "PL_ML_PLC_CONFUSION_MATRIX.csv"))
write.csv(CST_CM, file.path(EXPT_PL_Path, "REPORT", "PL_ML_CST_CONFUSION_MATRIX.csv"))

# make Calibration
PONO_NUM = nrow(unique(as.data.frame(test[, c("BAS_YM", "PONO")])))
CUST_NUM = nroM(unique(as.data.frame(test[, c("BAS_YM", "PLHD_INTG_CUST_NO")])))
PLC_CB_PROB <- makeCALIBRATION(ml=BEST_ML, newdata=test, breaks=seq(from=0,to=100,5), type="PLC")
CST_CB_PROB <- makeCALIBRATION(ml=BEST_ML, newdata=test, breaks=seq(from=0,to=100,5), type="CST")
PLC_CB_RANK <- makeCALIBRATION(ml=BEST_ML, newdata=test, m=PONO_NUM*0.05, type="PLC")
CST_CB_RANK <- makeCALIBRATION(ml=BEST_ML, newdata=test, m=CUST_NUM*0.05, type="CST")
write.csv(PLC_CB_PROB, file.path(EXPT_PL_Path, "REPORT", "PL_ML_PLC_CALIBRATION_PROB.csv"))
write.csv(PLC_CB_RANK, file.path(EXPT_PL_Path, "REPORT", "PL_ML_PLC_CALIBRATION_RANK.csv"))   
write.csv(CST_CB_PROB, file.path(EXPT_PL_Path, "REPORT", "PL_ML_CST_CALIBRATION_PROB.csv"))   
write.csv(CST_CB_RANK, file.path(EXPT_PL_Path, "REPORT", "PL_ML_CST_CALIBRATION_RANK.csv"))   

# save PL_ML_OUTPUT
test$Yhat <- predict(BEST_ML, newdata=test)[,3]
PL_ML_OUTPUT <- test[,c("PLHD_INTG_CUST_NO", "PONO", "PL_FLG", "Yhat", "TOT_PL_TIMES_MM11.CUMSUM")]
h2o.exportFile(PL_ML_OUTPUT, path=file.path(EXPT_PL_Path, "REPORT", "PL_ML_OUTPUT.csv"), force=T)

# print Log
cat("PL_ML_OUTPUT done!", "\n")
gc(reset = T)