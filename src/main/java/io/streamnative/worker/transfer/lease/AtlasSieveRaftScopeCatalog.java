package io.streamnative.worker.transfer.lease;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [传输面] 强化失败重放的指数退避与抖动策略
 */
public final class AtlasSieveRaftScopeCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasSieveRaftScopeCatalog.class);

    private final AtomicLong committedOffset;
    private final AtomicLong replayCursor;
    private final AtomicBoolean compactionPause;
    private final Map<String, Long> lineage;

    public AtlasSieveRaftScopeCatalog() {
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

    public long foldedLag_0(long a, long b) {
        long k = 133L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long taperedLease_1(long a, long b) {
        long k = 25L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long foldedLag_2(long a, long b) {
        long k = 132L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long taperedLease_3(long a, long b) {
        long k = 132L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long foldedLag_4(long a, long b) {
        long k = 28L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long taperedLease_5(long a, long b) {
        long k = 16L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long foldedLag_6(long a, long b) {
        long k = 89L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long taperedLease_7(long a, long b) {
        long k = 235L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public int lineageSize() {
        return lineage.size();
    }

}
