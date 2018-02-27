#!/usr/bin/env bash


SOURCE="${BASH_SOURCE[0]}"

while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  BINDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="BINDIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
BINDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

DIR=`dirname ${BINDIR}`
CLASSPATH="${DIR}/libs/*:/etc/hbase/conf:/etc/hadoop/conf:/etc/hive/conf"

echo "DIR=${DIR}"
export JAVA_HOME="/opt/jdk1.8.0_20"
export JAVA_HOME="/opt/jdk1.7.0_79"

rm -f ${DIR}/libs/slf4j-log4j12-*.jar
java -Daep.home.dir=${DIR} -cp  "${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar:${CLASSPATH}" com.ctg.aep.kerberostest.KafkaTestApplicaiton

