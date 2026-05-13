# pulsarMirrorMakerTool
```
java -cp '../lib/pulsar_mirror_maker_tool-2.0-SNAPSHOT-jar-with-dependencies.jar' \
-Djava.ext.dirs=../lib::$JAVA_HOME/jre/lib/ext/ \
-DtaskId=1 -DworkId=1 \
-Dlog4j.configurationFile=../conf/log4j2.xml \
-Djava.security.krb5.conf=../conf/krb5.conf -Djava.security.auth.login.config=../conf/jaas.conf \
io.streamnative.pulsar.PulsarMirrorMaker \
--listenPort 10001 \
--numKafkaConsumerInstances 2 \
--topicMappingList '{"topics":[{"inputTopics":["test-kakfa"],"outputTopic":"public/default/test-pulsar"}]}' \
--consumerPropertiesJson '{"bootstrap.servers":"localhost:9092","group.id.prefix":"kafka_to_pulsar_task_","max.poll.records":10000}' \
--pulsarClientConfJson '{"brokerServiceUrl":"pulsar://localhost:6650"}' \
--pulsarProducerConfJson '{"maxPendingMessages":1000}'
```


| Name | Description                                                           |
|------|-----------------------------------------------------------------------|
| `taskId ` | Every task has one unique taskId, will be used in consumer group name |
| `listenPort  ` | Listen port for metrics and control command                           |
| `topicMappingList   ` | Topic mapping list                                                    |
| `consumerPropertiesJson   ` | Kafka consumer conf                                                   |
| `pulsarClientConfJson   ` | Pulsar client conf                                                    |
| `pulsarProducerConfJson   ` | Pulsar producer                                                       |

After started, you can get metrics like this:

http://localhost:10000/metrics

You can stop this sync tool, using this command

http://localhost:10000/stop


### dockert
Manual packaging
* DockerFile
```
FROM java:8
WORKDIR /opt/pulsar_mirror_maker_tool-2.0-SNAPSHOT/bin
ADD pulsar_mirror_maker_tool-2.0-SNAPSHOT-bin.tar.gz /opt/
```
* build image

`docker build -f DockerFile -t pulsar_mirror_maker_tool:v2.0 .`

* run
```
  docker run -itd --name pulsar_mirror_maker_tool \
  -e listenPort=10001 \
  -e 'jvm=-Xms1g -Xmx1g' \
  -e taskId=2c918bd7809d362301809d5365ae0018 \
  -e numKafkaConsumerInstances=1 \
  -e workId=2c918bd7809d362301809d5365ae0018 \
  -e 'topicMappingList={"topics":[{"outputTopic":"public/default/testpulsar","inputTopics":["testpulsar","testpulsar"]}]}' \
  -e 'consumerPropertiesJson={"bootstrap.servers":"192.168.0.102:9092"}' \
  -e 'pulsarClientConfJson={"brokerServiceUrl":"pulsar://192.168.0.102:6651"}' \
  -e jaas_conf='S2Fma2FDbGllbnQgewogIGNvbS5zdW4uc2VjdXJpdHkuYXV0aC5tb2R1bGUuS3JiNUxvZ2luTW9kdWxlIHJlcXVpcmVkCiAgdXNlVGlja2V0Q2FjaGU9ZmFsc2UKICB1c2VLZXlUYWI9dHJ1ZQogIHNlcnZpY2VOYW1lPSJrYWZrYSIKICBzdG9yZUtleT10cnVlCiAgZGVidWc9dHJ1ZQogIGtleVRhYj0iL2V0Yy9zZWN1cml0eS9rZXl0YWJzL2thZmthLWNvbnN1bWVyLmtleXRhYiIKICBwcmluY2lwYWw9ImthZmthL2NvbnN1bWVyQFBVTFNBUi5DT00iOwp9OwoKUHVsc2FyUHJvZHVjZXIgewogICBjb20uc3VuLnNlY3VyaXR5LmF1dGgubW9kdWxlLktyYjVMb2dpbk1vZHVsZSByZXF1aXJlZAogICB1c2VLZXlUYWI9dHJ1ZQogICBzdG9yZUtleT10cnVlCiAgIHVzZVRpY2tldENhY2hlPWZhbHNlCiAgIGtleVRhYj0iL2V0Yy9zZWN1cml0eS9rZXl0YWJzL3Byb2R1Y2VyLmtleXRhYiIKICAgcHJpbmNpcGFsPSJwcm9kdWNlckBQVUxTQVIuQ09NIgogICBkZWJ1Zz10cnVlOwp9Ow==' \
  -e krb5_conf='IENvbmZpZ3VyYXRpb24gc25pcHBldHMgbWF5IGJlIHBsYWNlZCBpbiB0aGlzIGRpcmVjdG9yeSBhcyB3ZWxsCmluY2x1ZGVkaXIgL2V0Yy9rcmI1LmNvbmYuZC8KCltsb2dnaW5nXQogZGVmYXVsdCA9IEZJTEU6L3Zhci9sb2cva3JiNWxpYnMubG9nCiBrZGMgPSBGSUxFOi92YXIvbG9nL2tyYjVrZGMubG9nCiBhZG1pbl9zZXJ2ZXIgPSBGSUxFOi92YXIvbG9nL2thZG1pbmQubG9nCgpbbGliZGVmYXVsdHNdCiBkbnNfbG9va3VwX3JlYWxtID0gZmFsc2UKIHRpY2tldF9saWZldGltZSA9IDI0aAojIHJlbmV3X2xpZmV0aW1lID0gN2QKIGZvcndhcmRhYmxlID0gdHJ1ZQogcmRucyA9IGZhbHNlCiBwa2luaXRfYW5jaG9ycyA9IEZJTEU6L2V0Yy9wa2kvdGxzL2NlcnRzL2NhLWJ1bmRsZS5jcnQKIGRlZmF1bHRfcmVhbG0gPSBQVUxTQVIuQ09NCiBkZWZhdWx0X2NjYWNoZV9uYW1lID0gS0VZUklORzpwZXJzaXN0ZW50OiV7dWlkfQoKW3JlYWxtc10KIFBVTFNBUi5DT00gPSB7CiAga2RjID0gZGV6aGktMgogIGFkbWluX3NlcnZlciA9IGRlemhpLTIKIH0KIEtBRktBLkNPTSA9IHsKICBrZGMgPSBjaGluYXVuaW9uLTMKICBhZG1pbl9zZXJ2ZXIgPSBjaGluYXVuaW9uLTMKIH0KCltkb21haW5fcmVhbG1dCgpkZXpoaS0xLmludGVybmFsLmNsb3VkYXBwLm5ldCA9IEtBRktBLkNPTQpjaGluYXVuaW9uLTEuaW50ZXJuYWwuY2xvdWRhcHAubmV0ID0gUFVMU0FSLkNPTQpjaGluYXVuaW9uLTIuaW50ZXJuYWwuY2xvdWRhcHAubmV0ID0gUFVMU0FSLkNPTQpjaGluYXVuaW9uLTMuaW50ZXJuYWwuY2xvdWRhcHAubmV0ID0gUFVMU0FSLkNPTQo=' \
  -p 10001:10001 pulsar_mirror_maker_tool:v2.0
```
> jaas_conf and krb5_conf parameter is a string in base64 format

 # kopMirrorMakerTool

```
java -cp '../lib/pulsar_mirror_maker_tool-2.0-SNAPSHOT-jar-with-dependencies.jar' \
-Djava.ext.dirs=../lib::$JAVA_HOME/jre/lib/ext/ \
-DtaskId=1 -DworkId=1 \
-Dlog4j.configurationFile=../conf/log4j2.xml \
-Djava.security.krb5.conf=../conf/krb5.conf -Djava.security.auth.login.config=../conf/jaas.conf \
io.streamnative.pulsar.KopMirrorMaker \
--listenPort 10001 \
--numKafkaConsumerInstances 2 \
--topicMappingList '{"topics":[{"inputTopics":["test-kakfa"],"outputTopic":"public/default/test-pulsar"}]}' \
--consumerPropertiesJson '{"bootstrap.servers":"localhost:9092","group.id.prefix":"kafka_to_pulsar_task_","max.poll.records":10000}' \
--kopProducerConfJson '{"bootstrap.servers":"localhost:9093"}' 

```


| Name | Description                                                           |
|------|-----------------------------------------------------------------------|
| `taskId ` | Every task has one unique taskId, will be used in consumer group name |
| `listenPort  ` | Listen port for metrics and control command                           |
| `topicMappingList   ` | Topic mapping list                                                    |
| `consumerPropertiesJson   ` | Kafka consumer conf                                                   |
| `kopProducerConfJson   ` | Kafka Producer conf                                                   |

After started, you can get metrics like this:

http://localhost:10000/metrics

You can stop this sync tool, using this command

http://localhost:10000/stop


### dockert
Manual packaging
* DockerFile
```
FROM java:8
WORKDIR /opt/pulsar_mirror_maker_tool-2.0-SNAPSHOT/bin
ADD pulsar_mirror_maker_tool-2.0-SNAPSHOT-bin.tar.gz /opt/
```
* build image

`docker build -f DockerFile -t pulsar_mirror_maker_tool:v2.0 .`

* run
```
  docker run -itd --name pulsar_mirror_maker_tool \
  -e isKop=true \
  -e listenPort=10001 \
  -e 'jvm=-Xms1g -Xmx1g' \
  -e taskId=2c918bd7809d362301809d5365ae0018 \
  -e numKafkaConsumerInstances=1 \
  -e workId=2c918bd7809d362301809d5365ae0018 \
  -e 'topicMappingList={"topics":[{"outputTopic":"public/default/testpulsar","inputTopics":["testpulsar","testpulsar"]}]}' \
  -e 'consumerPropertiesJson={"bootstrap.servers":"192.168.0.102:9092"}' \
  -e 'kopProducerConfJson={"bootstrap.servers":"localhost:9093"}' \
  -e jaas_conf='S2Fma2FDbGllbnQgewogIGNvbS5zdW4uc2VjdXJpdHkuYXV0aC5tb2R1bGUuS3JiNUxvZ2luTW9kdWxlIHJlcXVpcmVkCiAgdXNlVGlja2V0Q2FjaGU9ZmFsc2UKICB1c2VLZXlUYWI9dHJ1ZQogIHNlcnZpY2VOYW1lPSJrYWZrYSIKICBzdG9yZUtleT10cnVlCiAgZGVidWc9dHJ1ZQogIGtleVRhYj0iL2V0Yy9zZWN1cml0eS9rZXl0YWJzL2thZmthLWNvbnN1bWVyLmtleXRhYiIKICBwcmluY2lwYWw9ImthZmthL2NvbnN1bWVyQFBVTFNBUi5DT00iOwp9OwoKUHVsc2FyUHJvZHVjZXIgewogICBjb20uc3VuLnNlY3VyaXR5LmF1dGgubW9kdWxlLktyYjVMb2dpbk1vZHVsZSByZXF1aXJlZAogICB1c2VLZXlUYWI9dHJ1ZQogICBzdG9yZUtleT10cnVlCiAgIHVzZVRpY2tldENhY2hlPWZhbHNlCiAgIGtleVRhYj0iL2V0Yy9zZWN1cml0eS9rZXl0YWJzL3Byb2R1Y2VyLmtleXRhYiIKICAgcHJpbmNpcGFsPSJwcm9kdWNlckBQVUxTQVIuQ09NIgogICBkZWJ1Zz10cnVlOwp9Ow==' \
  -e krb5_conf='IENvbmZpZ3VyYXRpb24gc25pcHBldHMgbWF5IGJlIHBsYWNlZCBpbiB0aGlzIGRpcmVjdG9yeSBhcyB3ZWxsCmluY2x1ZGVkaXIgL2V0Yy9rcmI1LmNvbmYuZC8KCltsb2dnaW5nXQogZGVmYXVsdCA9IEZJTEU6L3Zhci9sb2cva3JiNWxpYnMubG9nCiBrZGMgPSBGSUxFOi92YXIvbG9nL2tyYjVrZGMubG9nCiBhZG1pbl9zZXJ2ZXIgPSBGSUxFOi92YXIvbG9nL2thZG1pbmQubG9nCgpbbGliZGVmYXVsdHNdCiBkbnNfbG9va3VwX3JlYWxtID0gZmFsc2UKIHRpY2tldF9saWZldGltZSA9IDI0aAojIHJlbmV3X2xpZmV0aW1lID0gN2QKIGZvcndhcmRhYmxlID0gdHJ1ZQogcmRucyA9IGZhbHNlCiBwa2luaXRfYW5jaG9ycyA9IEZJTEU6L2V0Yy9wa2kvdGxzL2NlcnRzL2NhLWJ1bmRsZS5jcnQKIGRlZmF1bHRfcmVhbG0gPSBQVUxTQVIuQ09NCiBkZWZhdWx0X2NjYWNoZV9uYW1lID0gS0VZUklORzpwZXJzaXN0ZW50OiV7dWlkfQoKW3JlYWxtc10KIFBVTFNBUi5DT00gPSB7CiAga2RjID0gZGV6aGktMgogIGFkbWluX3NlcnZlciA9IGRlemhpLTIKIH0KIEtBRktBLkNPTSA9IHsKICBrZGMgPSBjaGluYXVuaW9uLTMKICBhZG1pbl9zZXJ2ZXIgPSBjaGluYXVuaW9uLTMKIH0KCltkb21haW5fcmVhbG1dCgpkZXpoaS0xLmludGVybmFsLmNsb3VkYXBwLm5ldCA9IEtBRktBLkNPTQpjaGluYXVuaW9uLTEuaW50ZXJuYWwuY2xvdWRhcHAubmV0ID0gUFVMU0FSLkNPTQpjaGluYXVuaW9uLTIuaW50ZXJuYWwuY2xvdWRhcHAubmV0ID0gUFVMU0FSLkNPTQpjaGluYXVuaW9uLTMuaW50ZXJuYWwuY2xvdWRhcHAubmV0ID0gUFVMU0FSLkNPTQo=' \
  -p 10001:10001 pulsar_mirror_maker_tool:v1.0
```
> jaas_conf and krb5_conf parameter is a string in base64 format