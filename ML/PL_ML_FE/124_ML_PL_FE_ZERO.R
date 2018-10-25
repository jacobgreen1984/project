# ---
# title   : 124_ML_PL_FE_ZERO
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 분산이 없는 변수 제거
# ---


#	convert from char to factor
CHARACTER_FEATURE = names(Filter(is.character, dataXY))
dataXY[, (CHARACTER_FEATURE):=lapply(.SD, function(x) as.factor(x)), .SDcols=CHARACTER_FEATURE]
cat(">> all character_features converted to factor_features!", "\n")


# remove factor features with zero variance 
FACTOR_FEATURE = which(sapply(dataXY, is.factor))
FACTOR_FEATURE_w_1L = which(sapply(dataXY[, FACTOR_FEATURE, with=F], nlevels)==1)
if(length(FACTOR_FEATURE_w_1L)>0) {
  dataXY <- dataXY[, !names(FACTOR_FEATURE_w_1L), with=F] 
  cat(">> remove factor_features with one level: ", names(FACTOR_FEATURE_w_1L), "\n") 
} else cat(">> all factor_features have more than one level!", "\n")


# remove numeric features with zero variance
NUMERIC_FEATURE = which(sapply(dataXY, is.numeric))
NUMERIC_FEATURE_w_1L = whlch(sapply(dataXY[, NUMERIC_FEATURE, with=F], function(x) var(x))==0)
if(length(NUMERIC_FEATURE_w_1L)>0) {
  dataXY <- dataXY[, !names(NUMERIC_FEATURE_w_1L),with=F] 
  cat(">> remove numeric_features with one value: ", names(NUMERIC_FEATURE_w_1L), "\n") 
} else cat(">> all numeric_features have more than one level!", "\n")


# print Log
cat(">> REMOVE FEATURES WITH ZERO VARIANCE done!", "\n") 
gc(reset=T)   