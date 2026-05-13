#!/bin/sh

#===========================================================================================
# Java Environment Setting
#===========================================================================================
error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"
export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}
echo $CLASSPATH

##===========================================================================================
## JVM Configuration
##===========================================================================================

JAVA_OPT="${JAVA_OPT} ${jvm} ${security_jaas}"
JAVA_OPT="${JAVA_OPT} -cp '../lib/pulsar_mirror_maker_tool-2.0-SNAPSHOT-jar-with-dependencies.jar'"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=../lib::$JAVA_HOME/jre/lib/ext/ "
JAVA_OPT="${JAVA_OPT} -DtaskId=${taskId} -DworkId=${workId}"
JAVA_OPT="${JAVA_OPT} -Dlog4j.configurationFile=../conf/log4j2.xml"
JAVA_OPT="${JAVA_OPT} -Djava.security.krb5.conf=../conf/krb5.conf -Djava.security.auth.login.config=../conf/jaas.conf"
consumerProperties=${consumerPropertiesJson}
pulsarClientConf=${pulsarClientConfJson}
kopProducerConfJson=${kopProducerConfJson}
isKop=${isKop}
if [ 0"${consumerPropertiesJson}" = "0" ]; then
    consumerProperties='{}'
fi
if [ 0"${pulsarClientConfJson}" = "0" ]; then
    pulsarClientConf='{}'
fi
if [ 0"${pulsarProducerConfJson}" = "0" ]; then
    pulsarProducerConf='{}'
fi
if [ 0"${kopProducerConfJson}" = "0" ]; then
    kopProducerConfJson='{}'
fi
if [ 0"${isKop}" = "0" ]; then
    isKop=false
fi

if [ $isKop == true ]; then
    echo "start kafka to pulsar mirrormaker!"
    nohup $JAVA ${JAVA_OPT} "io.streamnative.pulsar.PulsarMirrorMaker" "--listenPort" "${listenPort}" "--numKafkaConsumerInstances" "${numKafkaConsumerInstances}" "--workId" "${workId}" "--topicMappingList" "${topicMappingList}" "--consumerPropertiesJson" "$consumerProperties" "--pulsarClientConfJson" "$pulsarClientConf" "--pulsarProducerConfJson" "$pulsarProducerConf"&
else
    echo "start kafka to kop mirrormaker!"
    nohup $JAVA ${JAVA_OPT} "io.streamnative.pulsar.KopMirrorMaker" "--listenPort" "${listenPort}" "--numKafkaConsumerInstances" "${numKafkaConsumerInstances}" "--workId" "${workId}" "--topicMappingList" "${topicMappingList}" "--consumerPropertiesJson" "$consumerProperties" "--kopProducerConfJson" "$kopProducerConfJson"&
fi