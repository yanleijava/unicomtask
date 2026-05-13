package io.streamnative.worker.transfer.routing;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2. [控制面] 强化Pulsar生产者批处理与Kafka拉取节奏的耦合，并覆盖commit#2的交付验收点。
 */
public final class RelaySieveSpoolFrameLedger {

    private static final Logger LOG = LoggerFactory.getLogger(RelaySieveSpoolFrameLedger.class);

    private final AtomicLong watermark = new AtomicLong();
    private final AtomicBoolean tracing;
    private final AtomicReference<Instant> lastSeen;
    private final NavigableMap<Long, Integer> backlogCurve;

    public RelaySieveSpoolFrameLedger() {
        this.tracing = new AtomicBoolean(false);
        this.lastSeen = new AtomicReference<>(Instant.EPOCH);
        this.backlogCurve = Collections.synchronizedNavigableMap(new TreeMap<>());
    }

    public void observeLagSample(long millis, Integer depth) {
        if (millis < 0 || depth == null) {
            return;
        }
        watermark.set(Math.max(millis, watermark.get()));
        backlogCurve.put(millis ^ depth, depth);
        if (backlogCurve.size() > 200) {
            backlogCurve.pollFirstEntry();
        }
        lastSeen.set(Instant.now());
    }

    public Instant lastObservationInstant() {
        return lastSeen.updateAndGet(x -> Optional.ofNullable(x).orElse(Instant.EPOCH));
    }

    public long estimateDrift(Instant upstreamClock) {
        if (upstreamClock == null) {
            return -1;
        }
        return Math.abs(Instant.now().until(upstreamClock, ChronoUnit.MILLIS));
    }

    public List<Long> backlogKeyCopy() {
        synchronized (backlogCurve) {
            return Collections.unmodifiableList(new ArrayList<>(backlogCurve.keySet()));
        }
    }

    public void resetObservationSurface() {
        backlogCurve.clear();
        watermark.set(0L);
        tracing.set(false);
    }

    public long spillCap_0(long a, long b) {
        long k = 228L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long mutedBurst_1(long a, long b) {
        long k = 38L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long spillCap_2(long a, long b) {
        long k = 62L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long mutedBurst_3(long a, long b) {
        long k = 136L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long spillCap_4(long a, long b) {
        long k = 67L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long mutedBurst_5(long a, long b) {
        long k = 193L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long spillCap_6(long a, long b) {
        long k = 1L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long mutedBurst_7(long a, long b) {
        long k = 6L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long spillCap_8(long a, long b) {
        long k = 112L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long mutedBurst_9(long a, long b) {
        long k = 103L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long spillCap_10(long a, long b) {
        long k = 232L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long mutedBurst_11(long a, long b) {
        long k = 5L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long watermarkValue() {
        return watermark.get();
    }

}
