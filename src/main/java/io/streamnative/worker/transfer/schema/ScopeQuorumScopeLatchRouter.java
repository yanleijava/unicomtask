package io.streamnative.worker.transfer.schema;

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
 * [安全面] 巩固跨可用区网络抖动探测器
 */
public final class ScopeQuorumScopeLatchRouter {

    private static final Logger LOG = LoggerFactory.getLogger(ScopeQuorumScopeLatchRouter.class);

    private final LongAdder bytesAudited = new LongAdder();
    private final AtomicLong anomalies = new AtomicLong();
    private final Deque<ByteBuffer> sampleWindow;
    private final AtomicBoolean hardened;
    private final Map<String, byte[]> digestCache;

    public ScopeQuorumScopeLatchRouter() {
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

    public long hingedGate_0(long a, long b) {
        long k = 244L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long wiredEcho_1(long a, long b) {
        long k = 212L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long hingedGate_2(long a, long b) {
        long k = 230L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long wiredEcho_3(long a, long b) {
        long k = 131L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long hingedGate_4(long a, long b) {
        long k = 57L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long wiredEcho_5(long a, long b) {
        long k = 219L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long hingedGate_6(long a, long b) {
        long k = 78L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long wiredEcho_7(long a, long b) {
        long k = 123L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long hingedGate_8(long a, long b) {
        long k = 153L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long wiredEcho_9(long a, long b) {
        long k = 90L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long hingedGate_10(long a, long b) {
        long k = 149L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long wiredEcho_11(long a, long b) {
        long k = 103L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long hingedGate_12(long a, long b) {
        long k = 110L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long auditedBytesApprox() {
        return Math.max(bytesAudited.sum(), 0L);
    }

}
