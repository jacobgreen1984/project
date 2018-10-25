# ---
# title   : PL_CONF
# version : 0.1
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 configuration
# ---

# ---------
# 환경설정
# ---------
# 공통환경 Path
commonPath <- "/data/rpjt/R_script/COMMON"

#공통 환경, 라이브러리, 경로, 유저함수 모두 로드
for(i in file.path(commonPath,list.files(commonPath))) source(i)


# ---------
# Path
# ---------
# RDA 파일 경로
RDA_PL_Path  <- file.path(RDA_Path,"MDEL_PL_FRCST")

# HDFS - MDEL - PL_FRCST 경로 
MDEL_PL_Path  <- file.path(HDFS_Path,"data/MDEL/PL_FRCST")

# 모델 경로
EXPT_PL_Path  <- "data/EXPT_MDEL/MDEL_PL_FRCST"

# DM 경로
DM_PL_Path  <- "data/rpjt/R_script/MDEL_PL_FRCST/DM_script"

# ML 경로
ML_PL_Path  <- "data/rpjt/R_script/MDEL_PL_FRCST/ML_script"

# OP 경로
OP_PL_Path  <- "data/rpjt/R_script/MDEL_PL_FRCST/OP_script"

# ---------
# Library
# ---------
library(h2o)
library(stringr)
library(h2oEnsemble)
library(cvAUC)
library(ROCR)
library(data.table)
library(dplyr)
library(reshape2)
library(Hmisc)
library(caret)

# ---------
# Fucntion
# ---------
#####################################################
# Function name : getmode
# Description : 최빈값을 계산하는 함수
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
getmode <- function(v){
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v,uniqv)))]
}

#####################################################
# Function name : makeCusum
# Description : 누적변수 생성 함수
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeCusum <- function(var, from_MM00_to_MM0X){
  CMxFeature = paste0(var,"_MM",sprintf("%02d",from_MM00_to_MM0X),".CUMSUM")
  MMxFeature = saply(0:from_MM00_to_MM0X,function(j) paste0(var,"_MM",sprintf("%02d",j)))
  dataXY[,c(CMxFeature):=rowSums(.SD,na.rm = T),.SDcols=MMxFeature]
}

#####################################################
# Function name : removeMMxFeature
# Description : 변수명에 _MM이 포함된 변수 제거 함수
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
removeMMxFeature <- function(month){
  MMxFeature <- paste0("._MM",month,"$")
  dataXY[,-colnames(dataXY)[which(grepl(MMxFeature,colnames(dataXY)))],with=F]
}

#####################################################
# Function name : makeCustFeature
# Description : 계약변수를 사용하여 고객변수를 생성하는 함수 
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeCustFeature <- function(var, fun){
  if(fun=="MAX"){
    dataXY[,c(paste0("CST_",var,".MAX")) := list(max(get(var),na.rm = T)),by=list(PLHD_INTG_CUST_NO, BAS_YM)]
  }else if(fun=="MIN"){
    dataXY[,c(paste0("CST_",var,".MIN")) := list(min(get(var),na.rm = T)),by=list(PLHD_INTG_CUST_NO, BAS_YM)]
  }else if(fun=="SUM_MAX"){
    dataXY[,c(paste0("CST_",var,".SUM")) := list(sum(get(var),na.rm = T)),by=list(PLHD_INTG_CUST_NO, BAS_YM)]
    dataXY[,c(paste0("CST_",var,".MAX")) := list(max(get(var),na.rm = T)),by=list(PLHD_INTG_CUST_NO, BAS_YM)]
  }else if(fun=="SUM_MIN"){
    dataXY[,c(paste0("CST_",var,".SUM")) := list(sum(get(var),na.rm = T)),by=list(PLHD_INTG_CUST_NO, BAS_YM)]
    dataXY[,c(paste0("CST_",var,".MIN")) := list(min(get(var),na.rm = T)),by=list(PLHD_INTG_CUST_NO, BAS_YM)]
  }else{
    cat("valid values for fun are : MAX, MIN, SUM_MAX, SUM_MIN")
  }
  cat(var,"done!","\n")
}

#####################################################
# Function name : makeEdaPlot
# Description : 종속변수 기반 EDA 플랏을 생성하는 함수
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeEdaPlot <- function(var,y,path){
  features = data.frame(var=dataXY[,get(var),y=y])
  features = features %>% dplyr::count(var,y) %>% dplyr::group_by(var) %>% dplyr::mutate(prct=round(n/sum(n)*100,1))
  
  ggplot(features, aes(x=var,y=n,fill=y)) +
    geom_bar(stat="identity",width=0.5) +
    geom_text_repel(aes(label=paste0(n,"(",prct,"%)")),arrow=arrow(length=unit(0.02,"npc")),size=2) +
    theme(axis.text.x=element_text(angle=90)) +
    xlab(var) +
    ylab("count")
  
  ggsave(paste0(path,var,".png"))
  tryCatch(dev.off(),error=function(e)cat(""))
}

#####################################################
# Function name : makeCONFUSIONMATRIX
# Description : 계약별 고객별 혼동행렬을 산출하는 함수
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeCONFUSIONMATRIX <- function(ml,knewdata,type="CST"){
  if(type=="CST"){
    BestML_pred <- predict(ml, newdata=newdata)
    
    output <- data.frame(
      BAS_YM = as.data.frame(newdata$BAS_YM)[,1],
      PLHD_INTG_CUST_NO = as.data.frame(newdata$PLHD_INTG_CUST_NO)[,1],
      PL_FLG = as.numeric(as.data.frame(h2o.ifelse(newdata$PL_FLG=="Y",1,0))[,1]),
      Yhat = as.numeric(as.data.frame(h2o.ifelse(BestML_pred[,1]=="Y",1,0))[,1])
    )
    
    output <- as.data.table(output)
    output <- output[,lapply(.SD,max),by=list(BAS_YM,PLHD_INTG_CUST_NO),.SDcols=c("PL_FLG","Yhat")]
    output[,PL_FLG := ifelse(PL_FLG>=1,1,0)]
    output[,Yhat := ifelse(Yhat>=1,1,0)]
    output <- as.data.frame.matrix(table(output$PL_FLG,output$Yhat))
    print(output)
  }else if(type=="PLC"){
    output <- as.data.frame(h2o.confusionMatrix(h2o.performance(ml,newdata=newdata)))
    print(output)
  }
  else cat("type is CST or PLC, try again!")
}

#####################################################
# Function name : makeCALIBRATION
# Description : 예측확률을 구간화하는 함수 
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeCALIBRATION <- function(ml,newdata,breaks=NULL,m=NULL,type="CST"){
  
  # make prediction
  BestML_pred <- predict(ml, newdata=newdata)
  
  # make type
  if(type=="CST"){
    BestML_pred <- predict(ml, newdata=newdata)
    
    output_pred <- data.frame(
      BAS_YM = as.data.frame(newdata$BAS_YM)[,1],
      PLHD_INTG_CUST_NO = as.data.frame(newdata$PLHD_INTG_CUST_NO)[,1],
      Yhat = as.numeric(as.data.frame(BestML_pred[,3])[,1]),
      PL_FLG = as.numeric(as.data.frame(h2o.ifelse(newdata$PL_FLG=="Y",1,0))[,1]),
      LOAN_EXP = as.numeric(as.data.frame(h2o.ifelse(newdata$TOT_PL_TIMES_MM11.CUMSUM>0,1,0))[,1])
    )
    
    output_pred <- as.data.table(output_pred)
    output_pred <- output_pred[,lapply(.SD,max),by=list(BAS_YM,PLHD_INTG_CUST_NO),.SDcols=c("PL_FLG","Yhat","LOAN_EXP")]
    output_pred[,PL_FLG := ifelse(PL_FLG>=1,1,0)]
    output_pred[,LOAN_EXP := ifelse(LOAN_EXP>=1,1,0)]
    output_pred[,c("PLHD_INTG_CUST_NO","BAS_YM"):=NULL]
    
  }else if(type=="PLC"){
    newdata$LOAN_EXP = h2o.ifelse(newdata$TOT_PL_TIMES_MM11.CUMSUM>0,1,0)
    newdata$PL_FLG = h2o.ifelse(newdata$PL_FLG=="Y",1,0)
    output_pred = as.data.table(data.frame(
      PL_FLG = as.data.frame(newdata$PL_PLG)$PL_PLG,
      Yhat = as.data.frame(BestML_pred[,3])[,1],
      LOAN_EXP = as.data.frame(newdata$LOAN_EXP)$LOAN_EXP
    ))
    
  }else cat("type is CST or PLC, try again!")
  
  # cut probability
  if(!is.null(m)){
    output_pred[,RATE:=Hmisc::cut2(Yhat,m=m)]
  }else if(!is.null(breaks)){
    output_pred[,RATE:=cut(Yhat*100,breaks=breaks)]
  }else cat("please type agin!")
  output_pred[Yhat:=NULL]
  output_pred_melted = melt(output_pred,"RATE")
  
  # calculate mean/sum/count per rate
  output_avg = setNames(dcast.data.table(output_pred_melted,RATE~variable,fun.aggregate = mean), c("RATE","PL_FLG_AVG","LOAN_EXP_AVG"))
  output_sum = setNames(dcast.data.table(output_pred_melted,RATE~variable,fun.aggregate = sum), c("RATE","PL_FLG","LOAN_EXP"))
  output_cnt = setNames(dcast.data.table(output_pred_melted,RATE~variable,fun.aggregate = length), c("RATE","COUNT","LOAN_EXP"))
  output_cnt[,LOAN_EXP:=NULL]
  
  # make summary
  output = output_cnt[output_sum]
  output[,PL_FLG_Y:=PL_FLG]
  output[,PL_FLG_N:=COUNT-PL_FLG]
  output[,LOAN_EXP_Y:=LOAN_EXP]
  output[,LOAN_EXP_N:=COUNT-LOAN_EXP]
  output[,c("PL_FLG","LOAN_EXP"):=NULL]
  output = output[output_avg]
  output = output[order(output$RATE, decreasing = T),]
  output[,PL_FLG_AVG:=round(PL_FLG_AVG,4)]
  output[,LOAN_EXP_AVG:=round(LOAN_EXP_AVG,4)]
  
  # print output
  print(output)
  return(output)
}

#####################################################
# Function name : makeDataForStack
# Description : stacking모델을 위한 z-table을 생성하는 함수
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeDataForStack <- function(valid=valid, test=test, INTERVAL=0.1){
  # assign the best base_learner
  BEST_BL = which.max(gsub(".*\\_","",ML_PATH))
  
  if(INTERVAL!=0.5){
    # make stacking dataset with tag
    TRAIN_FOR_STACK     <- h2o.cbind(valid$PL_FLG, Reduce(h2o.cbind,VALID_PROB))
    TRAIN_FOR_STACK$TAG <- h2o.asfactor(h2o.ifelse(VALID_PROB[[BEST_BL]] >=0.5-INTERVAL & VALID_PROB[[BEST_BL]] <=0.5+INTERVAL,1,0))
    TEST_FOR_STACK      <- h2o.cbind(test$PL_FLG, Reduce(h2o.cbind,TEST_PROB))
    TEST_FOR_STACK$TAG  <- h2o.asfactor(h2o.ifelse(TEST_PROB[[BEST_BL]] >=0.5-INTERVAL & TEST_PROB[[BEST_BL]] <=0.5+INTERVAL,1,0))
  }else{
    # make stacking dataset without tag
    cat(">> if INTERVAL is",INTERVAL,"do not make TAG! <<","\n")
    TRAIN_FOR_STACK     <- h2o.cbind(valid$PL_FLG, Reduce(h2o.cbind,VALID_PROB))
    TEST_FOR_STACK      <- h2o.cbind(test$PL_FLG, Reduce(h2o.cbind,TEST_PROB))
  }
  # show Log
  cat("NEXT >> TRAIN, TEST Made With TAG using +-",INTERVAL,"INTERVAL From 0.5 Probability","\n")
  DATA_FOR_STACK <- list(train=TRAIN_FOR_STACK, test=TEST_FOR_STACK)
}

#####################################################
# Function name : makeLIFT
# Description : LIFT값을 산출하는 함수 
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeLIFT <- function(ml, newdata){
  tryCatch({
    Yhat                <- as.data.frame(predict(ml, newdata=newdata))[,3]
    Yact                <- as.data.frame(newdata$PL_FLG)$PL_FLG
    dataLIFT            <- data.frame(Yact,Yhat)
    dataLIFT            <- dataLIFT[order(dataLIFT$Yhat, decreasing = T),]
    rownames(dataLIFT)  <- NULL
    dataLIFT$decile     <- as.numeric(cut(as.numeric(rownames(dataLIFT)),breaks = 20))
    baseline            <- as.data.frame(h2o.table(newdata$PL_FLG))$Count[2]/sum(as.data.frame(h2o.table(newdata$PL_FLG))$Count)
    
    # compute Lift
    output                 <- setNames(as.data.frame(table(dataLIFT$decile)),c("decile","cases"))
    output$target          <- as.data.frame.matrix(table(dataLIFT$decile, dataLIFT$Yact))$Y
    output$'target(%)'     <- output$target/output$cases
    output$lift            <- output$'target(%)'/baseline
    output$cum_cases       <- cumsum(output$cases)
    output$cum_target      <- cumsum(output$target)
    output$'cum_target(%)' <- output$cum_target/output$cum_cases
    output$cum_lift        <- output$'cum_target(%)'/baseline
    rownaems(output)       <- NULL
  },error=function(e) cat("not enough sample for lift values","\n"))
  print(output)
}

#####################################################
# Function name : makeKS
# Description : KS값을 산출하는 함수 
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makeKS <- function(ml, newdata){
  Yhat   <- predict(ml,newdata=newdata)
  dataKS <- data.frame(Yhat=as.data.frame(Yhat)[,3], Yact=as.data.frame(newdata$PL_FLG)$PL_FLG)
  pred   <- ROCR::prediction(dataKS$Yhat, dataKS$Yact)
  perf   <- ROCR::performance(pred,"tpr","fpr")
  ks     <- max(attr(perf,"y.values")[[1]]-attr(perf,"x.values")[[1]])
  cat("BEST ML's KS: ",ks,"\n")
  return(ks)
}

#####################################################
# Function name : makePSI
# Description : PSI값을 산출하는 함수 
# Create date : 2017.11.07
# Create user : 2e consulting
# Last modification date : 2017.11.07
# Last modification user : 2e consulting
# Release : 1.0
# Release note
# 1.0 : 2017.11.07 등록
#####################################################
makePSI <- function(OLD_DATA,NEW_DATA){
  
  # extract group info from old dataset
  OLD_DATA <- data.table(as.data.frame(OLD_DATA[,c("P1")]))
  OLD_DATA[,GROUP := Hmisc::cut2(P1,g=10)]
  
  # extract group range from old dataset
  BREAK_POINTS <- OLD_DATA[,min(P1),by=GROUP]
  BREAK_POINTS <- BREAK_POINTS[order(GROUP),]
  BREAK_POINTS <- c(0, BREAK_POINTS$V1[-1],1)
  
  # make OLD_GROUP
  OLD_GROUP <- as.data.table(melt(OLD_DATA,"GROUP"))
  OLD_GROUP <- setNames(dcast.data.table(OLD_GROUP~variable, fun.aggregate=length),c("GROUP","COUNT"))
  OLD_GROUP[,GROUP_RATIO_OLD:=round(COUNT/sum(COUNT),4)]
  
  # make NEW_GROUP
  NEW_GROUP <- as.data.frame(NEW_DATA[,c("P1"),with=F])$P1
  NEW_GROUP <- as.data.frame.model.matrix(table(cut(NEW_GROUP,breaks = BREAK_POINTS)))
  NEW_GROUP <- data.frame(rownames(NEW_GROUP),NEW_GROUP)
  NEW_GROUP <- as.data.table(setNames(NEW_GROUP,c("GROUP","COUNT")))
  NEW_GROUP <- NEW_GROUP[order(GROUP, decreasing = T),]
  NEW_GROUP[,GROUP_RATIO_NEW:=as.numeric(round(COUNT/sum(COUNT),4))]
  
  # compute psi
  PSI_INDEX <- NEW_GROUP[,list(GROUP, GROUP_RATIO_NEW)]
  PSI_INDEX[,GROUP_RATIO_OLD:=OLD_GROUP$GROUP_RATIO_OLD]
  PSI_INDEX[,PSI_PER_RATE:=ifelse(GROUP_RATIO_NEW>0,(GROUP_RATIO_OLD-GROUP_RATIO_NEW)*log(GROUP_RATIO_OLD/GROUP_RATIO_NEW),0)]
  
  return(PSI_INDEX)
}

