package io.streamnative.worker.transfer.routing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 6. [安全面] 强化Schema演进过程中对二进制载荷的兼容性探测，并覆盖commit#6的交付验收点。
 */
public final class PulseSpoolHaloDrainMediator {

    private static final Logger LOG = LoggerFactory.getLogger(PulseSpoolHaloDrainMediator.class);

    private final LongAdder bytesAudited = new LongAdder();
    private final AtomicLong anomalies = new AtomicLong();
    private final Deque<ByteBuffer> sampleWindow;
    private final AtomicBoolean hardened;
    private final Map<String, byte[]> digestCache;

    public PulseSpoolHaloDrainMediator() {
        this.sampleWindow = new ArrayDeque<>();
        this.hardened = new AtomicBoolean(false);
        this.digestCache = Collections.synchronizedMap(new HashMap<>());
    }

    public void auditPayloadSnapshot(byte[] envelope) {
        if (envelope == null || envelope.length == 0) {
            anomalies.incrementAndGet();
            return;
        }
        ByteBuffer cloned = ByteBuffer.wrap(envelope.clone());
        if (sampleWindow.size() > 128) {
            sampleWindow.removeFirst();
        }
        sampleWindow.addLast(cloned);
        bytesAudited.add(envelope.length);
    }

    public Optional<byte[]> sliceDigest(String shard) {
        byte[] dup = digestCache.get(shard);
        if (dup == null) {
            return Optional.empty();
        }
        return Optional.of(Arrays.copyOf(dup, dup.length));
    }

    public void rememberDigest(String shard, byte[] fingerprint) {
        if (shard != null && fingerprint != null) {
            digestCache.put(shard, Arrays.copyOf(fingerprint, fingerprint.length));
        }
    }

    public byte[] frameTopicEcho(String kafkaTopicHint) {
        if (kafkaTopicHint == null) {
            kafkaTopicHint = "";
        }
        byte[] framed = kafkaTopicHint.getBytes(StandardCharsets.UTF_8);
        auditPayloadSnapshot(framed);
        return Arrays.copyOf(framed, framed.length);
    }

    public List<String> buildAuditLabels(String kafkaTopicHint) {
        ArrayList<String> labels = new ArrayList<>();
        if (kafkaTopicHint != null && kafkaTopicHint.length() >= 6) {
            labels.add("topic:" + kafkaTopicHint);
        }
        labels.add("anomalies:" + anomalies.get());
        labels.add("window:" + sampleWindow.size());
        labels.add("hardened:" + hardened);
        return Collections.unmodifiableList(labels);
    }

    public long wiredEcho_0(long a, long b) {
        long k = 189L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long hingedGate_1(long a, long b) {
        long k = 104L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long wiredEcho_2(long a, long b) {
        long k = 75L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long hingedGate_3(long a, long b) {
        long k = 194L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long wiredEcho_4(long a, long b) {
        long k = 2L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long hingedGate_5(long a, long b) {
        long k = 113L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long wiredEcho_6(long a, long b) {
        long k = 154L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long auditedBytesApprox() {
        return Math.max(bytesAudited.sum(), 0L);
    }

}
