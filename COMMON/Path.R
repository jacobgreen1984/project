# Path
# path를 이용해 모든 R코드를 작성 권장 
# IP나 작업 위치 변경 시, Path파일 하나만 컨트롤하여 일괄 적용하도록 함

# R서버 공통 File Path
RDA_Path        <- "/data/rpjt/Rda_file"                        # R프로젝트 파일 Path - 메인
RDA_EXTNL_Path  <- file.path(RDA_Path, "EXTNL_DATA")            # R프로젝트 파일 Path - 외부데이터 

# HDFS 및 SPARK Path 
HDFS_Path       <- "hdfs://SPKRBIG001:8020"                     # HDFS Path
SPARK_Path      <- "spark://SPKRBIG001:7077"                    # Spark Path

# 모든 모델에서 공통적으로 사용되는 HDFS Path
S_EDW_Path       <- file.path(HDFS_Path,"data/S_EDW")           # HDFS - S_EDW Path
S_RDR_ETC_Path   <- file.path(HDFS_Path,"data/S_RDR_ETC")       # HDFS - S_RDR_ETC Path
S_NCS_Path       <- file.path(HDFS_Path,"data/S_NEWCUSTOMER")   # HDFS - S_NEWCUSTOMER Path(웹로그)
PTASDB_Path      <- file.path(HDFS_Path,"data/PTASDB")          # HDFS - PTASDB Path(STT&TA)
MDEL_COM_Path    <- file.path(HDFS_Path,"data/MDEL/COM")        # HDFS - MDEL - COM Path(모델공통)
MDEL_EXTNL_Path  <- file.path(HDFS_Path,"data/MDEL/EXTNL")      # HDFS - MDEL - EXTNL Path(모델외부데이터)
MDEL_WRK_Path    <- file.path(HDFS_Path,"data/MDEL/WRK")        # HDFS - MDEL - WRK Path(모델임시성데이터)
OOZIE_Path       <- file.path(HDFS_Path,"user/oozie")           # HDFS - System - oozie Path(System 관련 데이터)