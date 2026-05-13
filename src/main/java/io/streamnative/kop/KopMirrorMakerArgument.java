/**
 *
 */
package io.streamnative.kop;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.streamnative.common.KafkaConsumerConfig;
import io.streamnative.common.KopProducerConfig;
import java.io.IOException;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;

@Data
public class KopMirrorMakerArgument {

//    @Parameter(
//            names = "--taskId",
//            description = "Kafka Mirror to Pulsar taskId, should be unique.",
//            required = true
//    )
    //This is a data sync task ID, should be unique.
    private String taskId;


    @Parameter(
            names = "--workId",
            description = "The work id used to mark the same task.",
            required = true
    )
    //The work id used to mark the same task, Mainly used for indicator statistics.
    private String workId;

    @Parameter(
            names = "--listenPort",
            description = "the http listen port will be used, http server will be served as metrics and control.",
            required = true
    )
    private int listenPort;

    @Parameter(
            names = "--numKafkaConsumerInstances",
            description = "Kafka consumer instances, each consumer instance will run on separate thread."
    )
    private int numKafkaConsumerInstances = 2;

    @Parameter(
            names = "--topicMappingList",
            description = "Kafka topics to pulsar topic mapping list in json format, for example : "
                    + "{\"topics\":[{\"inputTopics\":[\"testTopic1\",\"testTopic2\"],"
                    + "\"outputTopic\":\"public/default/test1\"},{\"inputTopics\":[\"testTopic3\",\"testTopic4\"],"
                    + "\"outputTopic\":\"public/default/test2\"}]}",
            required = true
    )
    private String topicMappingList;

    @Parameter(
            names = "--consumerPropertiesFile",
            description = "Kafka consumer properties file",
            required = false
    )
    private String kafkaConsumerPropertiesFile;

    @Parameter(
            names = "--consumerPropertiesJson",
            description = "Kafka consumer conf file",
            required = false
    )
    private String kafkaConsumerPropertiesJson;

    @Parameter(
            names = "--kopProducerConfFile",
            description = "Kop Producer conf file",
            required = false
    )
    private String kopProducerConfFile;

    @Parameter(
            names = "--kopProducerConfJson",
            description = "Kop Producer conf json",
            required = false
    )
    private String kopProducerConfJson;

    @Parameter(
            names = "--jvmGCMetricsLoggerClassName",
            description = "jvmGCMetricsLoggerClassName",
            required = false
    )
    private String jvmGCMetricsLoggerClassName = "io.streamnative.common.stats.JvmCMSGCMetricsLogger";

    @Parameter(names = "--help", help = true)
    private boolean help = false;

    public KopMirrorMakerArgument() {
        this.kafkaConsumerPropertiesFile = "";
        this.kopProducerConfFile = "";

        this.kafkaConsumerPropertiesJson = "";
        this.kopProducerConfJson = "";
    }

    public static KopMirrorMakerArgument parseArgument(String[] args) throws ParameterException {
        KopMirrorMakerArgument jArgs = new KopMirrorMakerArgument();
        JCommander cmd = JCommander.newBuilder()
                .addObject(jArgs)
                .build();
        cmd.parse(args);
        if (jArgs.help) {
            cmd.usage();
            return null;
        }
        return jArgs;
    }

    public KopMirrorMakerConfig getKopMirrorMakerConfig() throws IOException {
        KopMirrorMakerConfig mirrorMakerConfig = new KopMirrorMakerConfig();
        mirrorMakerConfig.listenPort = listenPort;
        mirrorMakerConfig.taskId = taskId;
        mirrorMakerConfig.workId = workId;
        mirrorMakerConfig.numKafkaConsumerInstances = numKafkaConsumerInstances;
        mirrorMakerConfig.kafkaConsumerConfig =
                new KafkaConsumerConfig(this.kafkaConsumerPropertiesFile, this.kafkaConsumerPropertiesJson);
        mirrorMakerConfig.kopProducerConfig =
                new KopProducerConfig(this.kopProducerConfFile, this.kopProducerConfJson);
        mirrorMakerConfig.parseMappingList(topicMappingList);
        mirrorMakerConfig.kafkaConsumerConfig.getProperties().setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                mirrorMakerConfig.kafkaConsumerConfig.getProperties().get("group.id.prefix") + taskId);
        mirrorMakerConfig.kafkaConsumerConfig.getProperties().remove("group.id.prefix");
        mirrorMakerConfig.kafkaConsumerConfig.getProperties().setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false");
        mirrorMakerConfig.jvmGCMetricsLoggerClassName = this.jvmGCMetricsLoggerClassName;
        return mirrorMakerConfig;
    }
}
