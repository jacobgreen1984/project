# ---
# title   : 412_OP_PL_FRCST_RPT_EVAL
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 운영평가 리포트 생성
# ---


#	make AUC
BEST_ML_AUC <- h2o.auc(h2o.performance(model=BEST_ML, newdata=test))
cat("BEST_ML's AUC: ", BEST_ML_AUC, "\n")

# make KS
KS = makeKS(ml=BEST_ML, newdata=test)
write.csv(KS, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_KS.csv")))

# make LIFT
LIFT = makeLIFT(ml=BEST_ML, newdata=test)
write.csv(LIFT, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_LIFT.csv")))

# make Confusion Matrix
PLC_CM = makeCONFUSIONMATRIX(ml=BEST_ML, newdata=test, type="PLC")
CST_CM = makeCONFUSIONMATRIX(ml=BEST_ML, newdata=test, type="CST")
write.csv(PLC_CM, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_EVL_PLC_CONFUSION_MATRIX.csv")))
write.csv(CST_CM, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_EVL_CST_CONFUSION_MATRIX.csv")))

# make Calibration(!!20등급!!)
PONO_NUM = nrow(unique(as.data.frame(test[,c("BAS_YM", "PONO")])))
CUST_NUM = nrow(unique(as.data.frame(test[,c("BAS_YM", "PLHD_INTG_CUST_NO")])))
PLC_CB_PROB <- makeCALIBRATION(ml=BEST_ML, newdata=test, breaks=seq(from=0, to=100, 5), type="PLC")
CST_CB_PROB <- makeCALIBRATION(ml=BEST_ML, newdata=test, breaks=seq(from=0, to=100, 5), type="CST")
PLC_CB_RANK <- makeCALIBRATION(ml=BEST_ML, newdata=test, m=PONO*0.05, type="PLC")
CST_CB_RANK <- makeCALIBRATION(ml=BEST_ML, newdata=test, m=PONO*0.05, type="CST")
write.csv(PLC_CB_PROB, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_PLC_CALIBRATION_PROB.csv")))
write.csv(PLC_CB_RANK, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_PLC_CALIBRATION_RANK.csv")))
write.csv(CST_CB_PROB, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_CST_CALIBRATION_PROB.csv")))
write.csv(CST_CB_RANK, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_CST_CALIBRATION_RANK.csv")))

# save PL_OP_EVL_OUTPUT
PL_OP_EVL_OUTPUT <- dataXY[,c("PLHD_INTG_CUST_NO", "BAS_YM", "PONO", "TOT_PL_TIMES_MM11.CUMSUM", "PL_FLG")]
PL_OP_EVL_OUTPUT[, Yhat := as.data.frame(predict(BEST_ML, newdata=test))[,3]]
fwrite(PL_OP_EVL_OUTPUT, file.path(EXPT_PL_Path, "REPORT", paste0(unique(dataXY$BAS_YM)[1], "_PL_OP_EVL_OUTPUT.csv")))

# print Log
cat(">> OP_EVL_OUTPUT done!", "\n") 

# reset
gc(reset=T) 
