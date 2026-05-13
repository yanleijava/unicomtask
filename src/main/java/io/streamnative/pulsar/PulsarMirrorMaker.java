/**
 *
 */
package io.streamnative.pulsar;

import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.stats.JvmGCMetricsLogger;
import org.apache.pulsar.common.stats.JvmMetrics;
import org.apache.pulsar.shade.io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PulsarMirrorMaker a tool to consume data from kafka and then send to pulsar.
 */
public class PulsarMirrorMaker {
    private static final Map<String, Class<? extends JvmGCMetricsLogger>> gcLoggerMap = new HashMap<>();
    private static final Logger log = LoggerFactory.getLogger(PulsarMirrorMaker.class);
    private ExecutorService executorService;
    private static ScheduledExecutorService scheduledService =
            Executors.newScheduledThreadPool(1, new DefaultThreadFactory("jvmThreadpool"));
    private PulsarMirrorMakerConfig mirrorMakerConfig;
    private List<PulsarMirrorThread> mirrorThreads = new ArrayList<PulsarMirrorThread>();
    private MirrorMakerHttpServer httpServer;
    private AtomicBoolean isClosed;
    private PulsarMirrorMakerStat stat;

    public PulsarMirrorMaker(PulsarMirrorMakerConfig config) {
        this.mirrorMakerConfig = config;
        this.isClosed = new AtomicBoolean(false);
        this.stat = new PulsarMirrorMakerStat(config.taskId, config.workId);
    }

    public void init() throws IOException {
        executorService = Executors.newFixedThreadPool(mirrorMakerConfig.getNumKafkaConsumerInstances(),
                new DefaultThreadFactory("consumerThreadPool"));
        for (int i = 0; i < mirrorMakerConfig.numKafkaConsumerInstances; i++) {
            try {
                Map<String, PulsarMirrorProducer> prodcuerMap = getPulsarProducer();
                KafkaMirrorConsumer consumer = new KafkaMirrorConsumer(mirrorMakerConfig.kafkaConsumerConfig, stat);
                mirrorThreads.add(new PulsarMirrorThread(consumer, prodcuerMap, isClosed, mirrorMakerConfig, stat));
            } catch (KafkaException | PatternSyntaxException e) {
                log.error("create kafka consumer failed, ", e);
                throw new IOException("create new kafka consumer failed");
            }
        }
        httpServer = new MirrorMakerHttpServer(mirrorMakerConfig.listenPort, isClosed);
    }

    private Map<String, PulsarMirrorProducer> getPulsarProducer()
            throws PulsarClientException {
        Map<String, PulsarMirrorProducer> prodcuerMap =
                new ConcurrentHashMap<String, PulsarMirrorProducer>();
        for (String targetTopic : mirrorMakerConfig.getAllTargetTopics()) {
            PulsarMirrorProducer producer = prodcuerMap.get(targetTopic);
            if (producer == null) {
                producer = new PulsarMirrorProducer(mirrorMakerConfig, targetTopic);
                prodcuerMap.put(targetTopic, producer);
            }
        }
      return prodcuerMap;
    }
    public void start() throws IOException {
        for (PulsarMirrorThread mirrorThread : mirrorThreads) {
            executorService.submit(mirrorThread);
        }
        try {
            httpServer.start();
        } catch (IOException e) {
            log.error("http server start failed, ", e);
            throw e;
        }
    }

    public void stop() {
        try {
            isClosed.set(true);
            Iterator<PulsarMirrorThread> iterThread = mirrorThreads.iterator();
            while (iterThread.hasNext()) {
                PulsarMirrorThread thread = iterThread.next();
                log.info("close kafka consumer[{}] for topics {}",
                        thread.getConsumer().getConsumer().groupMetadata().groupId(),
                        mirrorMakerConfig.kafkaSourceTopics);
                thread.close();
            }
            if (executorService != null) {
                executorService.shutdownNow();
            }
            if (httpServer != null) {
                httpServer.stop();
            }
            log.info("Pulsar mirror maker stopped");
        } catch (Exception e) {
            log.error("failed to shutdown mirror maker", e);
        }
        System.exit(0);
    }

    public static void main(String[] args) {
        PulsarMirrorMakerConfig mirrorMakerConfig;
        try {
            PulsarMirrorMakerArgument argument = PulsarMirrorMakerArgument.parseArgument(args);
            if (argument == null) {
                // help print
                return;
            }
            log.info("args: " + Arrays.asList(args));
            argument.setTaskId(System.getProperty("taskId"));
            log.info("arguments: {}", argument.toString());
            if (StringUtils.isEmpty(argument.getTaskId())) {
                log.error("Kafka Mirror to Pulsar taskId, can not null and should be unique.");
                System.exit(0);
            }
            mirrorMakerConfig = argument.getPulsarMirrorMakerConfig();
        } catch (ParameterException e) {
            log.error("Argument parse failed,", e);
            return;
        } catch (IOException e) {
            log.error("Parse config file failed", e);
            return;
        }

        PulsarMirrorMaker mirrorMaker = new PulsarMirrorMaker(mirrorMakerConfig);
        try {
            mirrorMaker.init();
        } catch (IOException e) {
            log.error("mirror init failed, ", e);
            mirrorMaker.stop();
            return;
        }
        try {
            mirrorMaker.start();
        } catch (IOException e) {
            log.error("pulsar mirror start failed, ", e);
            mirrorMaker.stop();
            return;
        }
        JvmMetrics jvmMetrics = JvmMetrics.create(scheduledService, "tool",
                mirrorMakerConfig.jvmGCMetricsLoggerClassName);

        while (!mirrorMaker.isClosed.get()) {
            try {
                int cnt = 1;
                while (!mirrorMaker.isClosed.get() && cnt < 30) {
                    Thread.sleep(1000);
                    cnt++;
                }
                mirrorMaker.stat.updateJVMmetrics(jvmMetrics.generate());
                for (PulsarMirrorThread mirrorThread : mirrorMaker.mirrorThreads) {
                    Map<TopicPartition, Long> lagMap = mirrorThread.lagMap;
                    for (Map.Entry<TopicPartition, Long> entry : lagMap.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        Long lag = entry.getValue();
                        mirrorMaker.stat.recordLagSize(tp.topic(), tp.partition()
                                , mirrorMakerConfig.kafkaConsumerConfig.getProperties()
                                        .getProperty(ConsumerConfig.GROUP_ID_CONFIG), lag);
                    }
                }
            } catch (InterruptedException e) {
                log.error("mirror maker exit interrupted, ", e);
            }
        }
        if (mirrorMaker.isClosed.get()) {
            mirrorMaker.stop();
        }
    }
}
