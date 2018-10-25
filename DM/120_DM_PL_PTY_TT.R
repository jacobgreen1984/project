# ---
# title   : 120_DM_PL_PTY_TT
# version : 1.0
# author  : 2e consulting
# desc.   : [템프테이블] DM_PL_PTY 생성을 위한 템프성 테이블
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
castToTimestamp(S_EDW_Path,"DW_CNTCT","CNTCT_ST_DTTM")
castToTimestamp(S_EDW_Path,"DW_CUST_INFO_CHG_DES","HSTY_EFTV_END_DTTM")     
castToTimestamp(S_EDW_Path,"DW_PYM_TRNS","PYM_DATE")
castToTimestamp(S_EDW_Path,"FT_PLC",c("LST_PLC_TRSF_DATE","LST_RNL_DATE"))     
castToTimestamp(S_EDW_Path,"TBL_CNSL_HSTR","CNSL_DTM")    

for(i in 1:length(looping_ar)){
  
  # --------------------------
  # set the time variables
  v_BAS_YM1    <- looping_ar[i]                    # 기준년월  (M시점)
  v_BAS_YM12   <- TRNS_MONTH.fn(V_BAS_YM1, -11)    # 기준년월12(M-11시점)
  v_FRCST_YM1  <- TRNS_MONTH.fn(V_BAS_YM1, +1)     # 예측년월12(M+1시점)
  v_FRCST_YM2  <- TRNS_MONTH.fn(V_BAS_YM1, +2)     # 예측년월12(M+2시점)
  
  # --------------------------
  # create DRIVING_TABLE
  PL_DRIVING_TABLE_TT <- SparkR::sql(paste0("
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
  createOrReplaceTempView(PL_DRIVING_TABLE_TT,"PL_DRIVING_TABLE_TT")
  
  # 고객단위로 재구성
  PL_DRIVING_TABLE <- SparkR::sql(paste0("
      SELECT    A1.BAS_YM                                                                /*기준년월*/
               ,A1.PLHD_INTG_CUST_NO                                                     /*계약자통합고객번호*/
      FROM      PL_DRIVING_TABLE_TT A1
      GROUP BY  A1.BAS_YM
               ,A1.PLHD_INTG_CUST_NO
  "))
  
  # --------------------------
  # Create Temp Table(1)
  PTY_ATTR_TT_01 <- SparkR::sql(paste0("
   SELECT     A1.BAS_YM                                                                   /*기준년월*/
             ,A1.PLHD_INTG_CUST_NO                         AS INTG_CUST_NO                /*계약자통합고객번호*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD = '30'
                       THEN 1
                  ELSE      0 END)                         AS INPLC_CNT                   /*유지계약건수*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD = '40'
                       THEN 1
                  ELSE      0 END)                         AS LPS_PLC_CNT                 /*실효계약건수*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD = '50'
                       THEN 1
                  ELSE      0 END)                         AS CNPLC_CNT                   /*해약계약건수*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD = '14'
                       THEN 1
                  ELSE      0 END)                         AS APP_RJT_CNT                 /*청약거절계약건수*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD IN ('16','18)
                       THEN 1
                  ELSE      0 END)                         AS WTDAL_PLC_CNT               /*청약철회계약건수*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD = '52' AND A1.CNLT_RSN_CD = '111'
                       THEN 1
                  ELSE      0 END)                         AS QUALY_GRTE_CNLT_CNT         /*품질보증해지계약건수*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD = '52' AND A1.CNLT_RSN_CD = '112'
                       THEN 1
                  ELSE      0 END)                         AS APEAL_CNPLC_CNT             /*민원해지계약건수*/
             ,SUM(CASE WHEN A1.PLC_STAT_CD NOT IN ('14','16','18','30','40','50','52')
                       THEN 1
                  ELSE      0 END)                         AS OT_PLC_CNT                  /*기타계약건수*/
   FROM       DM_PLC A1
   WHERE      A1.BAS_YM    = '",v_BAS_YM1,"'
   GROUP BY   A1.BAS_YM
             ,A1.PLHD_INTG_CUST_NO
  "))
  
  # --------------------------
  # 계약속성TT - 통합
  createOrRplaceTempView(PL_DRIVING_TABLE,"PL_DRIVING_TABLE")
  createOrRplaceTempView(PTY_ATTR_TT_01,"PTY_ATTR_TT_01")
  
  # 대출계약_TT
  PL_PTY_TT <- SparkR::sql(paste0("
     SELECT       A1.BAS_YM                           
                 ,A1.PLHD_INTG_CUST_NO
                 ,CAST(A2.INPLC_CNT               AS DOUBLE) AS INPLC_CNT                /*유지계약건수*/
                 ,CAST(A2.LPS_PLC_CNT             AS DOUBLE) AS LPS_PLC_CNT              /*실효계약건수*/
                 ,CAST(A2.CNPLC_CNT               AS DOUBLE) AS CNPLC_CNT                /*해약계약건수*/
                 ,CAST(A2.APP_RJT_CNT             AS DOUBLE) AS APP_RJT_CNT              /*청약거절계약건수*/
                 ,CAST(A2.WTDAL_PLC_CNT           AS DOUBLE) AS WTDAL_PLC_CNT            /*청약철회계약건수*/
                 ,CAST(A2.QUALY_GRTE_CNLT_CNT     AS DOUBLE) AS QUALY_GRTE_CNLT_CNT      /*품질보증해지계약건수*/
                 ,CAST(A2.APEAL_CNPLC_CNT         AS DOUBLE) AS APEAL_CNPLC_CNT          /*민원해지계약건수*/
                 ,CAST(A2.OT_PLC_CNT              AS DOUBLE) AS OT_PLC_CNT               /*기타계약건수*/

     FROM        PL_DRIVING_TABLE A1
     INNER JOIN  PLC_ATTR_TT_01  A2
             ON  A1.BAS_YM                            = A2.BAS_YM
            AND  A1.PLHD_INTG_CUST_NO                 = A2.INTG_CUST_NO

     WHERE       A1.BAS_YM               = '",v_BAS_YM1,"'
     ORDER BY    A1.PLHD_INTG_CUST_NO
                                        
                                        "))
  
  # --------------------------
  # Save to HDFS parquet
  WRK_table   <- "PL_PTY_TT"
  WRK_dir     <- file.path(gsub(HDFS_Path,"",MDEL_WRK_Path),WRK_table)
  
  # 기존 parquet파일 삭제 
  deleteExistParquetFile(WRK_dir, WRK_table, looping_ar[i])
  
  # parquet 파일 생성
  print(paste0("<< NOW : ",WRK_table," - ",looping_ar[i]," >>"))
  PL_PTY_TT  <- repartition(PL_PTY_TT, numPartitions=1L)
  
  write.df(PL_PTY_TT,WRK_dir, source ='parquet', mode='append')
  
  # 저장된 parquet 파일명 변경하기
  src <- grep(".snappy.parquet", hdfs.ls(WRK_dir)[hdfs.ls(WRK_dir)$modtime%in%max(hdfs.ls(WRK_dir)$modtime)]$file, value=TRUE)
  dest <- paste0(WRK_dir,'/',looping_ar[i],'_',WRK_table,'_',seq(src),'.parquet')
  for(p in seq(src)) hdfs.rename(src=src[p], dest=dest[p])
}

# chmod
system(paste0("hdfs dfs -chmod -R 777 ",MDEL_WRK_Path,"/",WRK_table))

print(paste0("<<< ",WRK_table," - DONE!! >>>"))
