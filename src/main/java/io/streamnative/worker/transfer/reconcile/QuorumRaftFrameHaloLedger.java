package io.streamnative.worker.transfer.reconcile;

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
 * [控制面] 巩固事务性消息与外置存储的状态机同步
 */
public final class QuorumRaftFrameHaloLedger {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumRaftFrameHaloLedger.class);

    private final AtomicLong watermark = new AtomicLong();
    private final AtomicBoolean tracing;
    private final AtomicReference<Instant> lastSeen;
    private final NavigableMap<Long, Integer> backlogCurve;

    public QuorumRaftFrameHaloLedger() {
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

    public long mutedBurst_0(long a, long b) {
        long k = 108L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long spillCap_1(long a, long b) {
        long k = 155L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long mutedBurst_2(long a, long b) {
        long k = 151L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long spillCap_3(long a, long b) {
        long k = 124L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long mutedBurst_4(long a, long b) {
        long k = 233L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long spillCap_5(long a, long b) {
        long k = 237L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long mutedBurst_6(long a, long b) {
        long k = 200L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long spillCap_7(long a, long b) {
        long k = 215L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long mutedBurst_8(long a, long b) {
        long k = 160L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long spillCap_9(long a, long b) {
        long k = 224L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long watermarkValue() {
        return watermark.get();
    }

}
