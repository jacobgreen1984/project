# ---
# title   : 123_ML_PL_FE_CUST
# version : 1.0
# author  : 2e consulting
# desc.   : ��������⿹���� �������� ����
# ---


#	convert to num
Integer_Features = names(Filter(is.integer, dataXY))
dataXY[,(Integer_Features) := lapply(.SD, function(x) as.numeric(x)), .SDcols=Integer_Features] 
cat(">> all integer_features converted to numeric_features!", "\n")

# setkey
setkey(dataXY, PLHD_INTG_CUST_NO, BAS_YM)
cat(">> CUSTOMER FEATURES start!", "\n")

# with MAX
makeCustFeature(var="MNCV_FCAMT_WANT", fun="MAX")                     # ����_�ְ�డ�Աݾ�
makeCustFeature(var="RID_FCAMT_WANT", fun="MAX")                      # ����_Ư�డ�Աݾ�
makeCustFeature(var="RTNN_NTHMM", fun="MAX")                          # ����_��������

# with SUM_MAX
makeCustFeature(var="CMIP", fun="SUM_MAX")                            # ����_CMIP
makeCustFeature(var="MNCV_CMIP", fun="SUM_MAX")                       # ����_�ְ���CMIP
makeCustFeature(var="RID_CMIP", fun="SUM_MAX")                        # ����_Ư��CMIP                                                                                                                          
makeCustFeature(var="PL_PSB_AMT", fun="SUM_MAX")                      # ����_���Ⱑ�ɱݾ�
makeCustFeature(var="PL_BLC", fun="SUM_MAX")                          # ����_�����ܾ�
makeCustFeature(var="SRDVLU_WANT", fun="SUM_MAX")                     # ����_�ؾ�ȯ�ޱ�
makeCustFeature(var="TOT_PAID_PRM", fun="SUM_MAX")                    # ����_�ѳ��Աݾ�
makeCustFeature(var="TOT_PAID_PRM_L1", fun="SUM_MAX")                 # ����_�ѳ��Աݾ�_1������

# cumulative sum features MM01
makeCustFeature(var="TOT_PL_AMT_MM01.CUMSUM", fun="SUM_MAX")          # ����_����ݾ�
makeCustFeature(var="TOT_PL_TIMES_MM01.CUMSUM", fun="SUM_MAX")        # ����_����Ƚ��
makeCustFeature(var="TOT_RDMT_AMT_MM01.CUMSUM", fun="SUM_MAX")        # ����_�ѻ�ȯ�ݾ� (����+�Ϻ�)                                                                                                                      
makeCustFeature(var="TOT_RDMT_TIMES_MM01.CUMSUM", fun="SUM_MAX")      # ����_�ѻ�ȯȽ�� (����+�Ϻ�)
makeCustFeature(var="LPS_TIMES_MM01.CUMSUM", fun="SUM_MAX")           # ����_��ȿȽ��
makeCustFeature(var="PRM_OVDU_TIMES_MM01.CUMSUM", fun="SUM_MAX")      # ����_����ῬüȽ��
makeCustFeature(var="PTWDL_PYM_AMT_MM01.CUMSUM", fun="SUM_MAX")       # ����_�ߵ��������ޱݾ�
makeCustFeature(var="PTWDL_PYM_TIMES_MM01.CUMSUM", fun="SUM_MAX")     # ����_�ߵ���������Ƚ��

# cumulative sum features MM02
makeCustFeature(var="TOT_PL_AMT_MM02.CUMSUM", fun="SUM_MAX")          # ����_����ݾ�
makeCustFeature(var="TOT_PL_TIMES_MM02.CUMSUM", fun="SUM_MAX")        # ����_����Ƚ��
makeCustFeature(var="TOT_RDMT_AMT_MM02.CUMSUM", fun="SUM_MAX")        # ����_�ѻ�ȯ�ݾ� (����+�Ϻ�)                                                                                                                            
makeCustFeature(var="TOT_RDMT_TIMES_MM02.CUMSUM", fun="SUM_MAX")      # ����_�ѻ�ȯȽ�� (����+�Ϻ�)
makeCustFeature(var="LPS_TIMES_MM02.CUMSUM", fun="SUM_MAX")           # ����_��ȿȽ��
makeCustFeature(var="PRM_OVDU_TIMES_MM02.CUMSUM", fun="SUM_MAX")      # ����_����ῬüȽ��
makeCustFeature(var="PTWDL_PYM_AMT_MM02.CUMSUM", fun="SUM_MAX")       # ����_�ߵ��������ޱݾ�
makeCustFeature(var="PTWDL_PYM_TIMES_MM02.CUMSUM", fun="SUM_MAX")     # ����_�ߵ���������Ƚ��

# cumulative sum features MM11
makeCustFeature(var="TOT_PL_AMT_MM11.CUMSUM", fun="SUM_MAX")          # ����_����ݾ� 
makeCustFeature(var="TOT_PL_TIMES_MM11.CUMSUM", fun="SUM_MAX")        # ����_����Ƚ��
makeCustFeature(var="TOT_RDMT_AMT_MM11.CUMSUM", fun="SUM_MAX")        # ����_�ѻ�ȯ�ݾ� (����+�Ϻ�)                                                                                                                                 
makeCustFeature(var="TOT_RDMT_TIMES_MM11.CUMSUM", fun="SUM_MAX")      # ����_�ѻ�ȯȽ�� (����+�Ϻ�)
makeCustFeature(var="LPS_TIMES_MM11.CUMSUM", fun="SUM_MAX")           # ����_��ȿȽ��
makeCustFeature(var="PRM_OVDU_TIMES_MM11.CUMSUM", fun="SUM_MAX")      # ����_����ῬüȽ��
makeCustFeature(var="PTWDL_PYM_AMT_MM11.CUMSUM", fun="SUM_MAX")       # ����_�ߵ��������ޱݾ�
makeCustFeature(var="PTWDL_PYM_TIMES_MM11.CUMSUM", fun="SUM_MAX")     # ����_�ߵ���������Ƚ��

# print Log
cat(">> CUSTOMER FEATURES done!", "\n") 
gc(reset=T)                