# ---
# title   : 123_ML_PL_FE_CUST
# version : 1.0
# author  : 2e consulting
# desc.   : º¸Çè°è¾à´ëÃâ¿¹Ãø¸ðµ¨ °í°´º¯¼ö »ý¼º
# ---


#	convert to num
Integer_Features = names(Filter(is.integer, dataXY))
dataXY[,(Integer_Features) := lapply(.SD, function(x) as.numeric(x)), .SDcols=Integer_Features] 
cat(">> all integer_features converted to numeric_features!", "\n")

# setkey
setkey(dataXY, PLHD_INTG_CUST_NO, BAS_YM)
cat(">> CUSTOMER FEATURES start!", "\n")

# with MAX
makeCustFeature(var="MNCV_FCAMT_WANT", fun="MAX")                     # °í°´_ÁÖ°è¾à°¡ÀÔ±Ý¾×
makeCustFeature(var="RID_FCAMT_WANT", fun="MAX")                      # °í°´_Æ¯¾à°¡ÀÔ±Ý¾×
makeCustFeature(var="RTNN_NTHMM", fun="MAX")                          # °í°´_À¯ÁöÂ÷¿ù

# with SUM_MAX
makeCustFeature(var="CMIP", fun="SUM_MAX")                            # °í°´_CMIP
makeCustFeature(var="MNCV_CMIP", fun="SUM_MAX")                       # °í°´_ÁÖ°³¾àCMIP
makeCustFeature(var="RID_CMIP", fun="SUM_MAX")                        # °í°´_Æ¯¾àCMIP                                                                                                                          
makeCustFeature(var="PL_PSB_AMT", fun="SUM_MAX")                      # °í°´_´ëÃâ°¡´É±Ý¾×
makeCustFeature(var="PL_BLC", fun="SUM_MAX")                          # °í°´_´ëÃâÀÜ¾×
makeCustFeature(var="SRDVLU_WANT", fun="SUM_MAX")                     # °í°´_ÇØ¾àÈ¯±Þ±Ý
makeCustFeature(var="TOT_PAID_PRM", fun="SUM_MAX")                    # °í°´_ÃÑ³³ÀÔ±Ý¾×
makeCustFeature(var="TOT_PAID_PRM_L1", fun="SUM_MAX")                 # °í°´_ÃÑ³³ÀÔ±Ý¾×_1°³¿ùÀü

# cumulative sum features MM01
makeCustFeature(var="TOT_PL_AMT_MM01.CUMSUM", fun="SUM_MAX")          # °í°´_´ëÃâ±Ý¾×
makeCustFeature(var="TOT_PL_TIMES_MM01.CUMSUM", fun="SUM_MAX")        # °í°´_´ëÃâÈ½¼ö
makeCustFeature(var="TOT_RDMT_AMT_MM01.CUMSUM", fun="SUM_MAX")        # °í°´_ÃÑ»óÈ¯±Ý¾× (Àü¾×+ÀÏºÎ)                                                                                                                      
makeCustFeature(var="TOT_RDMT_TIMES_MM01.CUMSUM", fun="SUM_MAX")      # °í°´_ÃÑ»óÈ¯È½¼ö (Àü¾×+ÀÏºÎ)
makeCustFeature(var="LPS_TIMES_MM01.CUMSUM", fun="SUM_MAX")           # °í°´_½ÇÈ¿È½¼ö
makeCustFeature(var="PRM_OVDU_TIMES_MM01.CUMSUM", fun="SUM_MAX")      # °í°´_º¸Çè·á¿¬Ã¼È½¼ö
makeCustFeature(var="PTWDL_PYM_AMT_MM01.CUMSUM", fun="SUM_MAX")       # °í°´_ÁßµµÀÎÃâÁö±Þ±Ý¾×
makeCustFeature(var="PTWDL_PYM_TIMES_MM01.CUMSUM", fun="SUM_MAX")     # °í°´_ÁßµµÀÎÃâÁö±ÞÈ½¼ö

# cumulative sum features MM02
makeCustFeature(var="TOT_PL_AMT_MM02.CUMSUM", fun="SUM_MAX")          # °í°´_´ëÃâ±Ý¾×
makeCustFeature(var="TOT_PL_TIMES_MM02.CUMSUM", fun="SUM_MAX")        # °í°´_´ëÃâÈ½¼ö
makeCustFeature(var="TOT_RDMT_AMT_MM02.CUMSUM", fun="SUM_MAX")        # °í°´_ÃÑ»óÈ¯±Ý¾× (Àü¾×+ÀÏºÎ)                                                                                                                            
makeCustFeature(var="TOT_RDMT_TIMES_MM02.CUMSUM", fun="SUM_MAX")      # °í°´_ÃÑ»óÈ¯È½¼ö (Àü¾×+ÀÏºÎ)
makeCustFeature(var="LPS_TIMES_MM02.CUMSUM", fun="SUM_MAX")           # °í°´_½ÇÈ¿È½¼ö
makeCustFeature(var="PRM_OVDU_TIMES_MM02.CUMSUM", fun="SUM_MAX")      # °í°´_º¸Çè·á¿¬Ã¼È½¼ö
makeCustFeature(var="PTWDL_PYM_AMT_MM02.CUMSUM", fun="SUM_MAX")       # °í°´_ÁßµµÀÎÃâÁö±Þ±Ý¾×
makeCustFeature(var="PTWDL_PYM_TIMES_MM02.CUMSUM", fun="SUM_MAX")     # °í°´_ÁßµµÀÎÃâÁö±ÞÈ½¼ö

# cumulative sum features MM11
makeCustFeature(var="TOT_PL_AMT_MM11.CUMSUM", fun="SUM_MAX")          # °í°´_´ëÃâ±Ý¾× 
makeCustFeature(var="TOT_PL_TIMES_MM11.CUMSUM", fun="SUM_MAX")        # °í°´_´ëÃâÈ½¼ö
makeCustFeature(var="TOT_RDMT_AMT_MM11.CUMSUM", fun="SUM_MAX")        # °í°´_ÃÑ»óÈ¯±Ý¾× (Àü¾×+ÀÏºÎ)                                                                                                                                 
makeCustFeature(var="TOT_RDMT_TIMES_MM11.CUMSUM", fun="SUM_MAX")      # °í°´_ÃÑ»óÈ¯È½¼ö (Àü¾×+ÀÏºÎ)
makeCustFeature(var="LPS_TIMES_MM11.CUMSUM", fun="SUM_MAX")           # °í°´_½ÇÈ¿È½¼ö
makeCustFeature(var="PRM_OVDU_TIMES_MM11.CUMSUM", fun="SUM_MAX")      # °í°´_º¸Çè·á¿¬Ã¼È½¼ö
makeCustFeature(var="PTWDL_PYM_AMT_MM11.CUMSUM", fun="SUM_MAX")       # °í°´_ÁßµµÀÎÃâÁö±Þ±Ý¾×
makeCustFeature(var="PTWDL_PYM_TIMES_MM11.CUMSUM", fun="SUM_MAX")     # °í°´_ÁßµµÀÎÃâÁö±ÞÈ½¼ö

# print Log
cat(">> CUSTOMER FEATURES done!", "\n") 
gc(reset=T)                