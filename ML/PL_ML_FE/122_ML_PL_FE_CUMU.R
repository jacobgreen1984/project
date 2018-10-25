# ---
# title   : 122_ML_PL_FE_CUMU
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 누적변수 생성
# ---


#	make cumulative features
MMxFeatures = unique(gsub("_MM.[0-9]", "", grep("_MM.[0-9]", colnames(dataXY), value=T)))

for(i in 1:length(MMxFeatures)){
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=1)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=2)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=3)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=4)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=5)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=6)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=7)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=8)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=9)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=10)
  makeCusum(var=MMxFeatures[i], from_MM00_to_MM0X=11)
}


# print Log
cat(">> CUMULATIVE FEATURES done!", "\n") 
gc(reset=T)

