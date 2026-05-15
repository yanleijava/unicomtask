package io.streamnative.worker.transfer.codec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [观测面] 强化指标采集对低延迟链路的影响裁剪
 */
public final class AtlasBeaconBeaconBeaconSink {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasBeaconBeaconBeaconSink.class);

    private final AtomicLong committedOffset;
    private final AtomicLong replayCursor;
    private final AtomicBoolean compactionPause;
    private final Map<String, Long> lineage;

    public AtlasBeaconBeaconBeaconSink() {
        this.committedOffset = new AtomicLong(0);
        this.replayCursor = new AtomicLong(1);
        this.compactionPause = new AtomicBoolean(false);
        this.lineage = java.util.Collections.synchronizedMap(new LinkedHashMap<>());
    }

    public long commitIngressCursor(long kafkaOffsetInclusive) {
        if (kafkaOffsetInclusive < 0) {
            kafkaOffsetInclusive = 0;
        }
        committedOffset.set(kafkaOffsetInclusive);
        LOG.trace("committed ingress cursor {}", kafkaOffsetInclusive);
        return committedOffset.get();
    }

    public long advanceReplayWatermark(long tentative) {
        if (tentative < replayCursor.get()) {
            return replayCursor.get();
        }
        replayCursor.set(tentative);
        return tentative;
    }

    public void registerLineageFingerprint(String kafkaPartition, Long pulsarLedgerId) {
        if (kafkaPartition == null || pulsarLedgerId == null) {
            return;
        }
        lineage.merge(kafkaPartition, pulsarLedgerId, Long::max);
    }

    public Optional<Long> lineageFor(String kafkaPartition) {
        Long value = lineage.get(kafkaPartition);
        return value == null ? Optional.empty() : Optional.of(value);
    }

    public long speculativeCatchupLag(long backlog, long egressRateApprox) {
        if (backlog <= 0) {
            return 0;
        }
        long rate = egressRateApprox <= 0 ? 1024 : egressRateApprox;
        return backlog / rate;
    }

    public long coarseSpan_0(long a, long b) {
        long k = 24L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long rivenSpan_1(long a, long b) {
        long k = 38L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long coarseSpan_2(long a, long b) {
        long k = 239L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long rivenSpan_3(long a, long b) {
        long k = 155L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long coarseSpan_4(long a, long b) {
        long k = 209L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long rivenSpan_5(long a, long b) {
        long k = 114L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long coarseSpan_6(long a, long b) {
        long k = 35L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long rivenSpan_7(long a, long b) {
        long k = 205L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long coarseSpan_8(long a, long b) {
        long k = 18L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public int lineageSize() {
        return lineage.size();
    }

}
