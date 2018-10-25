# ---
# title   : 110_DM_PL_PLC_TT
# version : 1.0
# author  : 2e consulting
# desc.   : [템프테이블] DM_PL_PLC 생성을 위한 템프성 테이블
# ---

# <<<<!---DM 구축 템플릿 공유를 위해 몇개의 샘플만 코드 작성---!>>>

# <<Data Load>> ----------------------------------
# Read HDFS Table 
readTablesFromSpark(S_EDW_Path, c("CO_CLG_YM"              # 마감년월
                                 ,"CO_DATE"                # CD 직종코드
                                 ,"CD_JBKND"               # 직업코드
                                 ,"CO_OCPN_CD"             # CD_관계코드
                                 ,"CD_RL"                  # 관계코드
                                 ,"CD_SALE_CHNL"           # 판매채널코드
                                 ,"DM_FC_BASIC"            # FC기본
                                 ,"DM_FC_STAT"             # FC상태 
                                 ,"DM_MKTG_PROD_GRP"       # 마케팅상품그룹
                                 ,"DM_PLC"                 # 계약
                                 ,"DM_PTY"                 # 당사자
                                 ,"DW_AUTO_TRSF"           # 자동이체
                                 ,"DW_AUTO_TRSF_CNLT_DES"  # 자동이체해지내역 
                                 ,"DW_CUST_ACNT_RL"        # 고객계좌관계
                                 ,"DW_INS"                 # 보험
                                 ,"DW_INS_PROD"            # 보험상품 
                                 ,"DW_INSD"                # 피보험자
                                 ,"DW_OWN_PLC"             # 본인계약
                                 ,"DW_PERPLC_DPS_TRNS_CLG" # 계약별입금TX마감내역
                                 ,"DW_PL"                  # 보험계약대출
                                 ,"DW_PL_MM_CLG"           # 보험계약대출월마감
                                 ,"DW_PL_BALC_MM_CLG"      # 보험계약대출잔고월마감
                                 ,"DW_PLC_TRNS_DES"        # 계약거래내역
                                 ,"DW_PYM_MM_CLG"          # 지급월마감
                                 ,"DW_PYM_TRNS"            # 지급거래
                                 ,"FT_CLAIM"               # FT청구
                                 ,"FT_PLC"                 # FT계약
                                 ,"FT_PLC_PYMT"            # FT계약납입
                                 ,"FT_VR_PLC"              # FT변액계약
                                 ,"AC_MEND_RSV"            # AC월말준비금
                                 ,"DW_INS_PLC_LOAN_INTRT"  # DW보험계약대출이율
                                 ,"DW_PAID_PRM_VRF"        # DW계약
                                 ,"DW_PRINSU_PYM_TRNS_DES" # DW금융거래지급
                                 ,"DW_EXRT_MGMT"           # 환율관리
                                 
))

# set timestamp(날짜변수를 TIMESTAMP 타입으로 변환)
castToTimestamp(S_EDW_Path,"CO_CLG_YM",c("CLG_ST_DATE","CLG_END_DATE"))
castToTimestamp(S_EDW_Path,"CO_DATE","STDATE")
castToTimestamp(S_EDW_Path,"CO_OCPN_CD","EFTV_END_DATE")     
castToTimestamp(S_EDW_Path,"DM_PLC","FST_PLC_DATE")
castToTimestamp(S_EDW_Path,"DW_AUTO_TRSF_CNLT_DES","CHG_PRC_DTTM")     
castToTimestamp(S_EDW_Path,"DW_CUST_ACNT_RL","HSTY_EFTV_END_DTTM")
castToTimestamp(S_EDW_Path,"DW_PERPLC_DPS_TRNS_CLG","DPS_DATE") 
castToTimestamp(S_EDW_Path,"DW_PL_MM_CLG",c("LST_LOAN_DATE","PYM_DATE")) 
castToTimestamp(S_EDW_Path,"DW_PYM_TRNS","PYM_DATE") 
castToTimestamp(S_EDW_Path,"DW_PRINSU_PYM","REG_DTTM") 
castToTimestamp(S_EDW_Path,"DW_EXRT_MGMT","STDATE") 


for(i in 1:length(looping_ar)){
  
  # --------------------------
  # set the time variables
  v_BAS_YM1    <- looping_ar[i]                    # 기준년월  (M시점)
  v_BAS_YM12   <- TRNS_MONTH.fn(V_BAS_YM1, -11)    # 기준년월12(M-11시점)
  v_FRCST_YM1  <- TRNS_MONTH.fn(V_BAS_YM1, +1)     # 예측년월12(M+1시점)
  v_FRCST_YM2  <- TRNS_MONTH.fn(V_BAS_YM1, +2)     # 예측년월12(M+2시점)
  
  # --------------------------
  # create DRIVING_TABLE
  PL_DRIVING_TABLE <- SparkR::sql(paste0("
      SELECT      A1.BAS_YM                                                              /*기준년월*/
                 ,A1.PONO                                                                /*증권번호*/
                 ,CASE WHEN A2.PYM_TIMES IS NULL
                       THEN 'N'
                  ELSE 'Y' END                                   AS PL_FLG               /*대출여부(TARGET)*/
                 ,A1.PLHD_INTG_CUST_NO                                                   /*계약자통합고객번호*/
                 ,A1.MINSD_INTG_CUST_NO                                                  /*주피보험자통합고객번호*/
                 ,A1.SLFC_NO                                                             /*모집FC번호*/
                 ,A1.SVFC_NO                                                             /*수금FC번호*/
      FROM        DM_PLC A1
      LEFT OUTER JOIN (
          SELECT  '",v_BAS_YM1,"'                                AS BAS_YM               /*기준년월*/
                  ,B1.PONO                                                               /*증권번호*/
                  ,COUNT(B3.PYM_DATE)                            AS PYM_TIMES            /*지급횟수*/
          FROM (
               SELECT  PYM_TRNS_ID
                      ,PONO
               FROM    DW_PYM_TRNS
               WHERE   PYM_PRC_CD       = 'TPL0'                                         /*지급처리 : 대출지급*/
                 AND   PYM_PRC_DETL_CD  IN ('PL0100','PL0200)                            /*지급거래상세 : 보험계약대출(신규,추가)*/
                 AND   DATE_FORMAT(PYM_DATE, 'yyyyMM')                                   /*지급일자기준*/
                       BETWEEN '",v_FRCST_YM1,"' AND '",v_FRCST_YM2,"'                   /*예측시점은 M+1~M+2시점*/
               EXCEPT
               SELECT  REF_TRNS_ID
                      ,PONO
               FROM    DW_PYM_TNS                                                        /*대출취소 지급거래내역 제외*/
               WHERE   PYM_PRC_CD       = 'TPL4'                                         /*지급처리 : 대출취소*/
               AND     PYM_PRC_DETL_CD  IN ('PL0100','PL0200)                            /*지급거래상세 : 보험계약대출(신규,추가)*/
          ) B1
          INNER JOIN DW_PYM_TRNS B2
                  ON B1.PYM_TRNS_ID    = B2.PYM_TRNS_ID
                 AND B1.PONO           = B2.PONO
          GROUP BY   B1.PONO
      ) A2
              ON  A1.BAS_YM            = A2.BAS_YM
             AND  A1.PONO              = A2.PONO
      INNER JOIN  DW_INS_PROD A3
              ON  A1.PROD_CD           = A3.PROD_CD
             AND  A1.PROD_VER_CD       = A3.PROD_VER_CD
      INNER JOIN  DW_PL A4
              ON  A1.PONO              = A4.PONO
      INNER JOIN  DM_PTY A5
              ON  A1.BAS_YM            = A5.BAS_YM
             AND  A1.PLHD_INTG_CUST_NO = A5.INTG_CUST_NO
      INNER JOIN  DM_FC_BASIC A6
              ON  A1.BAS_YM            = A6.BAS_YM
             AND  A1.SVFC_NO           = A6.FC_NO
      WHERE       A1.BAS_YM            = '",v_BAS_YM1,"'                                 /*기준년월*/
        AND      (A1.PLC_STAT_CD       = '30'                                            /*계약상태가 유지인 계약*/
                  OR (A1.PLC_STAT_CD   = '40'                                            /*계약상태가 실효이면서도 대출가능한 상품 추가*/
                      AND A1.FST_PLC_DATE < '2005-04-01'))                               /*최초청약일이 2005-04-01 이전인 실효상태의 계약*/
        AND       A1.PYMT_STAT_CD      != '06'                                           /*입금상태 완전납입면제(06) 제외*/
        AND      !(A1.FST_PLC_DATE     < '2007-11-26'                  
                   AND A3.PROD_REPT_TYP_CD LIKE '%V%')                                   /*최초청약일이 2007-11-26 이전이며 상품보고서유형코드에 V가 포함된 계약 제외*/
        AND       A3.PL_PSB_TYP_CD     != '01'                                           /*약관대출불가 상품 제외*/
        AND       A4.PL_TYP_CD         = '01'                                            /*보험계약대출(APL제외)*/
        AND       A5.CUST_DETL_TYP_CD  IN ('11','12','22')                               /*고객상세유형코드. 내국인(11),외국인(12),개인사업자(22)*/
        AND      !(A1.SFPLC_FLG        = 'Y'                                             /*현재활동중인FC본인계약 제외*/
                   AND A6.FC_STAT_CD   = '1')
    "))

  # --------------------------
  # Create Temp Table(1)
  PLC_ATTR_TT_01 <- SparkR::sql(paste0("
     SELECT     A1.BAS_YM                                                                   /*기준년월*/
               ,A1.PONO                                                                     /*증권번호*/
               ,A1.CMIP                                                                     /*CMIP*/
               ,A1.MNCV_FCANT_WAMT                                                          /*주계약가입금액원화환산금액*/
               ,A1.RID_FCAMT_WAMT                                                           /*특약가입금액원화환산금액*/
               ,A1.PMFQY_CD                                                                 /*납입주기코드*/
               ,CASE WHEN A1.PYMT_STAT_CD = '00'
                     THEN 'Y'
                ELSE      'N' END                               AS PRM_PYG_FLG              /*보험료납입중여부*/
               ,CASE WHEN A1.PYMT_STAT_CD = '02'
                     THEN 'Y'
                ELSE      'N' END                               AS MMPD_FLG                 /*월대체여부*/
               ,A1.PLC_STAT_CD                                                              /*계약상태코드*/
               ,A1.RTNN_NTHMM                                                               *유지차월*/
               ,CASE WHEN A1.RBNPLC_TYP_CD = '01'
                     THEN 'N'
                ELSE      'Y' END                               AS RBNPLC_FLG               /*고아계약여부*/
               ,CASE WHEN A1.PLHD_PLC_AGE = 999
                     THEN NULL
                ELSE      A1.PLHD_PLC_AGE END                   AS PLHD_PLC_AGE             /*계약자계약연령*/
               ,CASE WHEN A1.PLHD_CUR_AGE = 999
                     THEN NULL
                ELSE      A1.PLHD_CUR_AGE END                   AS PLHD_CUR_AGE             /*계약자현재연령*/
               ,A1.PLHD_GEN_CD                                                              /*계약자성별코드*/
               ,A1.PLHD_OCPN_CD                                                             /*계약자직업코드*/
               ,A1.PLHD_OCPN_RSKLVL_CD                                                      /*계약자직업위험등급코드*/
               ,A1.MINSD_PLC_AGE                                                            /*주피보험자계약연령*/
               ,A1.MINSD_GEN_CD                                                             /*주피보험자성별코드*/
               ,A1.MINSD_OCPN_CD                                                            /*주피보험자직업코드*/
               ,A1.MINSD_OCPN_RSKLVL_CD                                                     /*주피보험자직업위험등급코드*/
               ,A1.PMPMD_CD                                                                 /*수금방법코드*/
               ,A1.PLHD_SAME_FLG                                AS PLHD_MINSD_SAME_FLG      /*계약자주피보험자동일여부*/
               ,A1.HFAMDS_APLC_FLG                                                          /*고액할인적용여부*/
               ,A1.MDEXM_FLG                                                                /*건강진단여부*/
               ,CASE WHEN A1.PLHD_CUR_AGE  = 999
                       OR A1.MINSD_CUR_AGE = 999
                       OR A4.UPP_CD_EFTV_VLU NOT IN ('TA','GA')
                     THEN NULL
                ELSE      A1.PLHD_CUR_AGE-A1.MINSD_CUR_AGE END  AS PLHD_MINSD_CUR_AGE_DIFF  /*계약자주피보험자현재연령차이*/
               ,A2.MNCV_PYMT_PRD_TYP_CD                                                     /*주계약납입기간유형코드*/
               ,A2.MNCV_INS_PRD_TYP_CD                                                      /*주계약보험기간유형코드*/
               ,A2.MNCV_CMIP                                                                /*주계약CMIP*/
               ,A3.RID_CMIP                                                                 /*특약CMIP*/
               ,A3.RID_CNT                                                                  /*특약건수*/
               ,A4.UPP_CD_EFTV_VLU                              AS SALE_CHNL_L2_CD          /*판매채널레벨2코드*/
               ,A5.UPP_CD_EFTV_VLU                              AS PLC_RL_L1_CD             /*계약관계레벨1코드*/
               ,A6.KLIA_INS_KND_CD                                                          /*생명보험협회보험종류코드*/
               ,A6.PROD_REPT_TYP_CD                                                         /*상품보고서유형코드*/
               ,CASE WHEN A7.PROD_GRP_L3_CD IN ('P10107','P10110')
                     THEN 'Y'
                ELSE      'N' END                               AS ITSTV_PROD_FLG           /*금리연동형상품여부*/
               ,A7.PROD_GRP_L2_CD                               AS MKTG_PROD_GRP_L2_CD      /*마케팅상품그룹레벨2코드*/
               ,A7.PROD_GRP_L3_CD                               AS MKTG_PROD_GRP_L3_CD      /*마케팅상품그룹레벨3코드*/
               ,A7.PROD_GRP_L4_CD                               AS MKTG_PROD_GRP_L4_CD      /*마케팅상품그룹레벨4코드*/
               ,A8.TRSF_HOPE_DAY                                                            /*이체희망일*/
               ,CASE WHEN A9.PONO IS NOT NULL
                     THEN 'Y'
                ELSE      'N' END                               AS FC_FAM_PLC_FLG           /*FC가족계약여부*/
               ,NVL(A10.PL_BLC,0)                                                           /*대출잔액*/

     FROM DM_PLC A1                                                                         /*DM_계약*/
     INNER JOIN(
         SELECT    B1.PONO
                  ,B1.CMIP                                      AS MNCV_CMIP                /*주계약CMIP*/
                  ,B1.PYMT_PRD_TYP_CD                           AS MNCV_PYMT_PRD_TYP_CD     /*주계약납입기간유형코드*/
                  ,B1.INS_PRD_TYP_CD                            AS MNCV_INS_PRD_TYP_CD      /*주계약보험기간유형코드*/
         FROM      DW_INS B1
         WHERE     B1.BAS_YM              = '",v_BAS_YM1,"'
           AND     B1.INS_SRLNO           = 0
     ) A2
             ON A1.PONO                  = A2.PONO
     LEFT OUTER JOIN(
         SELECT    B1.PONO
                  ,SUM(B1.CMIP)                                 AS RID_CMIP                 /*특약CMIP*/ 
                  ,COUNT(B1.INS_SRLNO)                          AS RID_CNT                  /*특약수*/ 
         FROM      DW_INS B1
         WHERE     B1.BAS_YM             = '",v_BAS_YM1,"'
           AND     B1.INS_SRLNO          != 0
         GROUP BY  B1.PONO
     ) A3
             ON A1.PONO                  = A3.PONO
     INNER JOIN CD_SALE_CHNL A4                                                             /*CD_판매채널코드*/
             ON A1.SALE_CHNL_CD          = A4.CD_EFTV_VLU
     INNER JOIN CD_RL A5                                                                    /*CD_관계코드*/
             ON A1.PLHD_RL_CD            = A5.CD_EFTV_VLU
     INNER JOIN DW_INS_PROD A6                                                              /*DW_보험상품*/
             ON A1.PROD_CD               = A6.PROD_CD
            AND A1.PROD_VER_CD           = A6.PROD_VER_CD
     INNER JOIN DM_MKTG_PROD_GRP A7                                                         /*DM_마케팅상품그룹*/
             ON A6.PROD_GRP_CD           = A7.PROD_GRP_CD
     INNER JOIN FT_PLC_PYMT A8                                                              /*FT_계약납입*/
             ON A1.BAS_YM                = A8.BAS_YM
            AND A1.PONO                  = A8.PONO
     LEFT OUTER JOIN (
         SELECT    DISTINCT B1.PONO
         FROM      DW_OWN_PLC B1
         WHERE     B1.CLG_YM             = '",v_BAS_YM1,"'
     ) A9
             ON A1.PONO                  = A9.PONO
     LEFT OUTER JOIN (
         SELECT    B1.PONO
                  ,SUM(B1.PL_BLC)                               AS PL_BLC                   /*대출잔액*/
         FROM      DW_PL_BALC_MM_CLG B1
         WHERE     B1.CLG_YM             = '",v_BAS_YM1,"'
           AND     B1.PL_TYP_CD          = '01'
           AND     B1.PL_BLC             > 0
         GROUP BY  B1.PONO
     ) A10
             ON A1.PONO                  = A10.PONO
     
     WHERE     A1.BAS_YM                 = '",v_BAS_YM1,"'                                  /*기준년월*/

  "))

  # --------------------------  
  # 계약속성_TT - Roll_up_SUM
  # 부활횟수
  PLC_ATTR_TT_SUM_query  <- mkSpreadTermQuery('DM_PLC', 'RINST_TIMES','PONO','SUM','BAS_YM',v_BAS_YM1,-12)
  PLC_ATTR_TT_SUM_01     <- SparkR::sql(PLC_ATTR_TT_SUM_query)
  # 보험료추가납입금액
  PLC_ATTR_TT_SUM_query  <- mkSpreadTermQuery('DW_PERPLC_DPS_TRNS_CLG', 'PYMT_PRM','PONO','SUM','CLG_YM',v_BAS_YM1,-12,"DPS_PRC_CD IN ('02','03')", output_col_name="PRM_TUPMT_AMT")
  PLC_ATTR_TT_SUM_02     <- SparkR::sql(PLC_ATTR_TT_SUM_query)

  # 생성한 쿼리의 수 
  SUM_query_cnt <- 2
  
  # --------------------------
  # 계약속성_TT - Roll_up_COUNT
  # 실효횟수
  PLC_ATTR_TT_COUNT_query  <- mkSpreadTermQuery('DM_PLC', '1','PONO','COUNT','BAS_YM',v_BAS_YM1,-12,"PLC_STAT_CD='40'",output_col_name="LPS_TIMES")
  PLC_ATTR_TT_COUNT_01     <- SparkR::sql(PLC_ATTR_TT_COUNT_query)
  # 보험료연체횟수
  PLC_ATTR_TT_COUNT_query  <- mkSpreadTermQuery('DM_PLC', '1','PONO','COUNT','BAS_YM',v_BAS_YM1,-12,"DPS_PRC_CD IN ('02','03')", output_col_name="PRM_OVDU_TIMES")
  PLC_ATTR_TT_COUNT_02     <- SparkR::sql(PLC_ATTR_TT_COUNT_query)
  
  # 생성한 쿼리의 수 
  COUNT_query_cnt <- 2
  
  # --------------------------
  # 계약속성TT - 통합
  createOrRplaceTempView(PL_DRIVING_TABLE,"PL_DRIVING_TABLE")
  createOrRplaceTempView(PLC_ATTR_TT_01,"PLC_ATTR_TT_01")
  
  # Roll_up_변수(MM00~MM11) 조인쿼리 생성 
  sum_column_names    <- data.frame()
  for(j in 1:SUM_query_cnt){
    n <- ifelse(nchar(j)==1, paste0("0",j),j)
    eval(parse(text=paste0("createOrReplaceTempView(PLC_ATTR_TT_SUM_",n,", 'PLC_ATTR_TT_SUM_",n,"')")))
    eval(parse(text=paste0("sum_column_names <- rbind(sum_column_names,data.frame(table='PLC_ATTR_TT_SUM_"
                           ,n,"',column=columns(PLC_ATTR_TT_SUM_",n,")))")))
  }
  count_column_names  <- data.frame()
  for(k in 1:COUNT_query_cnt){
    m <- ifelse(nchar(k)==1, paste0("0",k),k)
    eval(parse(text=paste0("createOrReplaceTempView(PLC_ATTR_TT_COUNT_",m,", 'PLC_ATTR_TT_COUNT_",m,"')")))
    eval(parse(text=paste0("sum_column_names <- rbind(sum_column_names,data.frame(table='PLC_ATTR_TT_COUNT_"
                           ,m,"',column=columns(PLC_ATTR_TT_COUNT_",m,")))")))
  }
  sum_column_names    <- sum_column_names[which(sum_column_names$column!='PONO'),]
  count_column_names  <- count_column_names[which(count_column_names$column!='PONO'),]
  cal_column_names    <- rbind(sum_column_names,count_column_names)
  cal_table_names     <- unique(cal_column_names$table)
  
  cal_column_query      <-  ""
  cal_column_join_query <-  ""
  for(l in seq(length(cal_column_names$column)))
    cal_column_query  <- paste0(cal_column_query, sprintf(",%s.%s", cal_column_names$table[l], cal_column_names$column[l]))
  for(t in seq(length(cal_table_names)))
    cal_column_join_query  <- paste0(cal_column_join_query, sprintf(" LEFT OUTER JOIN %s ON A1.PONO=%s.PONO", cal_table_names[t], cal_table_names[t]))
  
  # 대출계약_TT
  PL_PLC_TT <- SparkR::sql(paste0("
      SELECT       A1.BAS_YM                           
                  ,A1.PONO
                  ,A1.PL_FLG
                  ,A1.PLHD_INTG_CUST_NO
                  ,A1.MINSD_INTG_CUST_NO
                  ,A1.SLFC_NO
                  ,A1.SVFC_NO
                  ,CAST(A2.CMIP                            AS DOUBLE) AS CMIP
                  ,CAST(A2.MNCV_FCANT_WAMT                 AS DOUBLE) AS MNCV_FCANT_WAMT          /*주계약가입금액원화환산금액*/
                  ,CAST(A2.RID_FCAMT_WAMT                  AS DOUBLE) AS A2.RID_FCAMT_WAMT        /*특약가입금액원화환산금액*/
                  ,CAST(A2.PMFQY_CD                        AS STRING) AS PMFQY_CD                 /*납입주기코드*/
                  ,CAST(A2.PRM_PYG_FLG                     AS STRING) AS PRM_PYG_FLG              /*보험료납입중여부*/
                  ,CAST(A2.MMPD_FLG                        AS STRING) AS MMPD_FLG                 /*월대체여부*/
                  ,CAST(A2.PLC_STAT_CD                     AS STRING) AS PLC_STAT_CD              /*계약상태코드*/
                  ,CAST(A2.RTNN_NTHMM                      AS INT)    AS RTNN_NTHMM               /*유지차월*/
                  ,CAST(A2.RBNPLC_FLG                      AS STRING) AS RBNPLC_FLG               /*고아계약여부*/
                  ,CAST(A2.PLHD_PLC_AGE                    AS INT)    AS PLHD_PLC_AGE             /*계약자계약연령*/
                  ,CAST(A2.PLHD_CUR_AGE                    AS INT)    AS PLHD_CUR_AGE             /*계약자현재연령*/
                  ,CAST(A2.PLHD_GEN_CD                     AS STRING) AS PLHD_GEN_CD              /*계약자성별코드*/
                  ,CAST(A2.PLHD_OCPN_CD                    AS STRING) AS PLHD_OCPN_CD             /*계약자직업코드*/
                  ,CAST(A2.PLHD_OCPN_RSKLVL_CD             AS STRING) AS PLHD_OCPN_RSKLVL_CD      /*계약자직업위험등급코드*/
                  ,CAST(A2.MINSD_PLC_AGE                   AS INT)    AS MINSD_PLC_AGE            /*주피보험자계약연령*/
                  ,CAST(A2.MINSD_GEN_CD                    AS STRING) AS MINSD_GEN_CD             /*주피보험자성별코드*/
                  ,CAST(A2.MINSD_OCPN_CD                   AS STRING) AS MINSD_OCPN_CD            /*주피보험자직업코드*/
                  ,CAST(A2.MINSD_OCPN_RSKLVL_CD            AS STRING) AS MINSD_OCPN_RSKLVL_CD     /*주피보험자직업위험등급코드*/
                  ,CAST(A2.PMPMD_CD                        AS STRING) AS PMPMD_CD                 /*수금방법코드*/
                  ,CAST(A2.PLHD_MINSD_SAME_FLG             AS STRING) AS PLHD_MINSD_SAME_FLG      /*계약자주피보험자동일여부*/
                  ,CAST(A2.HFAMDS_APLC_FLG                 AS STRING) AS HFAMDS_APLC_FLG          /*고액할인적용여부*/
                  ,CAST(A2.MDEXM_FLG                       AS STRING) AS MDEXM_FLG                /*건강진단여부*/
                  ,CAST(A2.PLHD_MINSD_CUR_AGE_DIFF         AS INT)    AS PLHD_MINSD_CUR_AGE_DIFF  /*계약자주피보험자현재연령차이*/
                  ,CAST(A2.MNCV_PYMT_PRD_TYP_CD            AS STRING) AS MNCV_PYMT_PRD_TYP_CD     /*주계약납입기간유형코드*/
                  ,CAST(A2.MNCV_INS_PRD_TYP_CD             AS STRING) AS MNCV_INS_PRD_TYP_CD      /*주계약보험기간유형코드*/
                  ,CAST(A2.MNCV_CMIP                       AS DOUBLE) AS MNCV_CMIP                /*주계약CMIP*/
                  ,CAST(A2.RID_CMIP                        AS DOUBLE) AS RID_CMIP                 /*특약CMIP*/
                  ,CAST(A2.RID_CNT                         AS INT)    AS RID_CNT                  /*특약건수*/
                  ,CAST(A2.SALE_CHNL_L2_CD                 AS STRING) AS SALE_CHNL_L2_CD          /*판매채널레벨2코드*/
                  ,CAST(A2.PLC_RL_L1_CD                    AS STRING) AS PLC_RL_L1_CD             /*계약관계레벨1코드*/
                  ,CAST(A2.KLIA_INS_KND_CD                 AS STRING) AS KLIA_INS_KND_CD          /*생명보험협회보험종류코드*/
                  ,CAST(A2.PROD_REPT_TYP_CD                AS STRING) AS PROD_REPT_TYP_CD         /*상품보고서유형코드*/
                  ,CAST(A2.ITSTV_PROD_FLG                  AS STRING) AS ITSTV_PROD_FLG           /*금리연동형상품여부*/
                  ,CAST(A2.MKTG_PROD_GRP_L2_CD             AS STRING) AS MKTG_PROD_GRP_L2_CD      /*마케팅상품그룹레벨2코드*/
                  ,CAST(A2.MKTG_PROD_GRP_L3_CD             AS STRING) AS MKTG_PROD_GRP_L3_CD      /*마케팅상품그룹레벨3코드*/
                  ,CAST(A2.MKTG_PROD_GRP_L4_CD             AS STRING) AS MKTG_PROD_GRP_L4_CD      /*마케팅상품그룹레벨4코드*/
                  ,CAST(A2.TRSF_HOPE_DAY                   AS INT)    AS TRSF_HOPE_DAY            /*이체희망일*/
                  ,CAST(A2.FC_FAM_PLC_FLG                  AS STRING) AS FC_FAM_PLC_FLG           /*FC가족계약여부*/
                  ,CAST(A2.PL_BLC                          AS DOUBLE) AS PL_BLC                   /*대출잔액*/

      FROM        PL_DRIVING_TABLE A1
      INNER JOIN  PLC_ATTR_TT_01  A2
              ON  A1.BAS_YM               = A2.BAS_YM
             AND  A1.PONO                 = A2.PONO
                  ",cal_column_join_query,"
      WHERE       A1.BAS_YM               = '",v_BAS_YM1,"'
      ORDER BY    A1.PONO
                                        
  "))
  
  # --------------------------
  # Save to HDFS parquet
  WRK_table   <- "PL_PLC_TT"
  WRK_dir     <- file.path(gsub(HDFS_Path,"",MDEL_WRK_Path),WRK_table)
    
  # 기존 parquet파일 삭제 
  deleteExistParquetFile(WRK_dir, WRK_table, looping_ar[i])
  
  # parquet 파일 생성
  print(paste0("<< NOW : ",WRK_table," - ",looping_ar[i]," >>"))
  PL_PLC_TT  <- repartition(PL_PLC_TT, numPartitions=1L)
  
  write.df(PL_PLC_TT,WRK_dir, source ='parquet', mode='append')
  
  # 저장된 parquet 파일명 변경하기
  src <- grep(".snappy.parquet", hdfs.ls(WRK_dir)[hdfs.ls(WRK_dir)$modtime%in%max(hdfs.ls(WRK_dir)$modtime)]$file, value=TRUE)
  dest <- paste0(WRK_dir,'/',looping_ar[i],'_',WRK_table,'_',seq(src),'.parquet')
  for(p in seq(src)) hdfs.rename(src=src[p], dest=dest[p])
}

# chmod
system(paste0("hdfs dfs -chmod -R 777 ",MDEL_WRK_Path,"/",WRK_table))

print(paste0("<<< ",WRK_table," - DONE!! >>>"))
