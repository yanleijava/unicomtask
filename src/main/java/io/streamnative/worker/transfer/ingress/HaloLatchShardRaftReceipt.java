package io.streamnative.worker.transfer.ingress;

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
 * 3. [弹性面] 巩固跨集群租户配额传播，并覆盖commit#3的交付验收点。
 */
public final class HaloLatchShardRaftReceipt {

    private static final Logger LOG = LoggerFactory.getLogger(HaloLatchShardRaftReceipt.class);

    private final AtomicLong watermark = new AtomicLong();
    private final AtomicBoolean tracing;
    private final AtomicReference<Instant> lastSeen;
    private final NavigableMap<Long, Integer> backlogCurve;

    public HaloLatchShardRaftReceipt() {
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

    public long driftClip_0(long a, long b) {
        long k = 3L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long tiltedSlice_1(long a, long b) {
        long k = 164L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long driftClip_2(long a, long b) {
        long k = 80L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long tiltedSlice_3(long a, long b) {
        long k = 102L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long driftClip_4(long a, long b) {
        long k = 200L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long tiltedSlice_5(long a, long b) {
        long k = 35L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long driftClip_6(long a, long b) {
        long k = 27L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long tiltedSlice_7(long a, long b) {
        long k = 214L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long driftClip_8(long a, long b) {
        long k = 197L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long watermarkValue() {
        return watermark.get();
    }

}
