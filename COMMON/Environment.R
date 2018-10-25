
# Environment
setwd("~")                                                                                       # 작업디렉토리를 개인 home으로 설정
Sys.setenv(LANG          = "en")                                                                 # 에러를 영어로 출력
Sys.setenv(LANGUAGE      = "ko-KR")
Sys.setenv(LC_ALL        = "ko_KR.UTF-8")
Sys.setenv(LC_CTYPE      = "Ko_KR.UTF-8")
options(stringsAsFactors = FALSE)                                                                # 데이터 Import 시 String을 Factor로 자동인식 방지
options(digits           = 15)                                                                   # Output 출력시 소수점 15자리 제한
options(scipen           = 999)                                                                  # Output 출력시 e로 표현되지 않도록 함

# rhdfs 패키지로 HDFS 접근 시 HADOOP_CMD의 경로 필요
Sys.setenv(HADOOP_CMD    = "/srv/ASAP/hadoop/2.7.2/bin/hadoop")

# Set JAVA_HOME
Sys.setenv(JAVA_HOME     = "/srv/ASAP/java/1.8.91")

# Standalone 모드는 클러스터 내에서 독립적으로 자원을 사용함
# Yarn-client 모드는 클러스터의 자원을 관리하는 Yarn 위에서 동작
Sys.setenv(SPARK_HOME = strsplit(system("cat /etc/profile|grep 'export SPARK_HOME'"
                                        ,intern=TRUE), "=")[[1]][2])                             # Spark Standalone Mode
# Sys.setenv(SPARK_HOME = "usr/hdp/current/spark2-client")                                       # Spark Yarn-client Mode
Sys.setenv(SPARKR_SUBMIT_ARGS = "--packages com.databricks:spark-csv_2.11:1.5.0 sparkr-shell")   # Spark에서 csv 읽기를 위한 jar 파일 로드
Sys.setenv(SPARKR_SUBMIT_ARGS = "--packages com.databricks:spark-csv_2.11:1.5.0 --conf spark.rpc.message.maxSize=512 
           --conf spark.kryoserializer.buffer.max=512 sparkr-shell")

local({r <- getOption("repos")                                                                   # 패키지 다운 시, CRAN MIRROR Site 주소로 변경
      r["CRAN"]        <- "http://10.25.3.12:80"                                                 # local CRAN server IP
      r["CRANextra"]   <- "http://10.25.3.12:80"
      options(repos = r)})
