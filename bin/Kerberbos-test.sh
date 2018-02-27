#!/usr/bin/env bash

if [ $# -ne 3 ]
then
    echo "usage $0 use_cache use_jaas tasks"
	exit 1
fi

SOURCE="${BASH_SOURCE[0]}"

while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  BINDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="BINDIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
BINDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

DIR=`dirname ${BINDIR}`
CLASSPATH="${DIR}/conf:${DIR}/libs/*:/etc/hbase/conf:/etc/hadoop/conf:/etc/hive/conf"

echo "DIR=${DIR}"

export JAVA_HOME="/opt/jdk1.7.0_79"

rm -f ${DIR}/libs/slf4j-log4j12-*.jar
rm -f logs/aep*log

#DEBUG_OPT="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9999"

CACHE_FILE="/etc/kafka/conf/kafka_odp_jaas_cache.conf"
NOCACHE_JAAS="/etc/kafka/conf/kafka_odp_jaas_keytab.conf"

if [ "X$1" == "0" ]
then
	KEYTAB_FILE=$NOCACHE_JAAS
else
	KEYTAB_FILE=$CACHE_FILE
fi

WITH_ENV=" -Djava.security.auth.login.config=${KEYTAB_FILE}"
WITHOUT_ENV=""

if [ "X$2" == "0" ]
then
	JAAS_ENV=$WITH_ENV
else
	JAAS_ENV=$WITHOUT_ENV
fi


echo java ${DEBUG_OPT} ${JAAS_ENV} -Daep.home.dir=${DIR} -cp  "${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar:${CLASSPATH}" com.ctg.aep.dataserver.AllInOne "$@"

java ${DEBUG_OPT} ${JAAS_ENV} -Daep.home.dir=${DIR} -cp  "${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar:${CLASSPATH}" com.ctg.aep.dataserver.AllInOne "$@"
