# ===
# titie   : 211_ML_PL_BL_BF
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 변수선택
# ===

# XGB
XGB_FOR_VI <= h2o.xgboost(
  training_frame = train,
  validation_frame = valid,
  x = x,
  y = y,
  seed = 1234,
  
  #overfitting options
  categorlcal_encoding = "EnumLimited", # 더미변수화하는 갯수를 최대 10개로 제한하고 11번째를 others로 마무리
  learn_rate = 0.1,	                    # optimal weight를 찾아가기 위해 계산되는 step direction 이후 step size를 얼마로? 크게하면 OW를 지나감
  max_abs_leafnode_pred =	2,	          #첫번째 트리의 성능이 너무 좋아 단독으로 사용되는 것을 방지하기 위해 최소 2개의 트리를 사용하도록 함
  min_rows= 5,	                        # 트리를 나누기 전에 가지고 있어야 할 최소 계약 갯수(기본은 1)
          
  # stoping options
  ntrees = 1000,	                      # 트리갯수를 일단 많이 넣어둔다 왜냐하면 아래있는 stopping rule에 의해 그 전에 최적 auc를 찾으므로.
  stopping_rounds	=	3,                  # stopping_tolerance의 score가 0.1%보다 큰 경우가 발생시 몇번 참을 것인가? 3번?
  score_tree_interval = 6,              # 각 tree별로 scoring을 한다(score_each_iteration = T 가 score_tree_interval = 1 과 같은 표현) 숫자가 커지면 과적합 가능성 있음
  stopping_tolerance = 1e-4,	          # 각 트리단계의 score 차이가 0.1%보다 발생하지 않을 경우 트리 stop
  stopping_metric = "AUC"	              # 분류모델에서는 AUC, 회귀모델에서는 잔차제곱의 합(RSS)을 쓴다.
)          
  # report
  print(tail(data.frame(XGB_FOR_VI@model$scoring_history)[c("number_of_trees","training_auc","validation_auc")]))
  cat(">> AUC(XGB): ",h2o.auc(h2o.performance(XGB_FOR_VI,newdata=test)),"\n")

# extract imporotance_features
VI_FROM_XGB <- unique(gsub("\\.top_.*","",h2o.varimp(XGB_FOR_VI)$variable))
VI_FROM_XGB <- unique(gsub("\\.N","",VI_FROM_XGB))
VI_FROM_XGB <- unique(gsub("\\_CD.*","_CD",VI_FROM_XGB))
VI_FROM_XGB <- unique(gsub("\\_FLG.*","_FLG",VI_FROM_XGB))
VI_FROM_XGB <- unique(gsub("TRSF_HOPE_DAY.*","TRSF_HOPE_DAY",VI_FROM_XGB))
cat("» VI NUMBER: ",length(VI_FROM_XGB),"\n")
           

# find best features from VI FROH XGB
NUM_FEATURE = unique(c(seq(from=50, to=length(VI_FROM_XGB),by=50),length(VI_FROM_XGB)))
BEST_FEATURE = list()
for(i in l:length(NUM_FEATURE)){
  
  XGB_FOR_VI <= h2o.xgboost(
    training_frame = train,
    validation_frame = valid,
    x = VI_FROM_XGB[1:NUM_FEATURE[i]],
    y = y,
    seed = 1234,

    #overfitting options 
    categorlcal_encoding = "EnumLimited",
    learn_rate = 0.1,
    max_abs_leafnode_pred =2,
    min_rows= 5,
    
    #stoping options 
    ntrees = 1000,
    stopping_rounds = 3,
    score_tree_interval = 6,
    stopping_tolerance = 1e-4,
    stopping_metric = "AUC"
  )
    
  BEST_FEATURE[[i]] <- data.frame(
  NUM_FEATURE = NUM_FEATURE[i],
  AUC = H2o.auc(h2o.performance(XGB_FOR_VI, newdata=test))
  )
}

# report
BEST_FEATURE_REPORT = rbindlist(BEST_FEATURE)
print(BEST_FEATURE_REPORT)

# save
write.csv(VI_FROM_XGB,file.path(EXPT_PL_Path, "REPORT", "PL_ML_VX_FROH_XGB.csv"))
write.csv(BEST_FEATURE_REPORT,file.path(EXPT_PL_Path, "REPORT","PL_ML_BEST_FEATURE_REPORT.csv"))

# print Log
cat(">> BEST FEATURES: TOP",BEST_FEATURE_REPORT[AUC==max(AUC),NUM_FEATURE],"\n")
gc(reset=T)
           
