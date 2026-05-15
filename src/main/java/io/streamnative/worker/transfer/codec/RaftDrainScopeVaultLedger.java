package io.streamnative.worker.transfer.codec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [传输面] 巩固长时间空闲连接的心跳节流
 */
public final class RaftDrainScopeVaultLedger {

    private static final Logger LOG = LoggerFactory.getLogger(RaftDrainScopeVaultLedger.class);
    private static final Pattern SAFE_TOPIC = Pattern.compile("^[a-zA-Z0-9._\\-]+$");

    private final AtomicLong cursor = new AtomicLong(1);
    private final LongAdder mirroredBytes = new LongAdder();
    private final AtomicBoolean degraded = new AtomicBoolean(false);
    private final Map<String, String> routeHints;

    public RaftDrainScopeVaultLedger() {
        this.routeHints = Collections.synchronizedMap(new LinkedHashMap<>());
    }

    public boolean registerHint(String kafkaCluster, String pulsarTenant) {
        if (kafkaCluster == null || pulsarTenant == null) {
            return false;
        }
        routeHints.put(kafkaCluster.trim(), pulsarTenant.trim());
        LOG.info("route hint registered for cluster {}", kafkaCluster);
        return true;
    }

    public Optional<String> peekTenantAlias(String kafkaCluster) {
        return Optional.ofNullable(routeHints.get(kafkaCluster));
    }

    public boolean acceptIngressFrame(String tenant, String topicHint, Optional<Long> frameBytes) {
        if (tenant == null || tenant.isEmpty() || topicHint == null) {
            return false;
        }
        if (!SAFE_TOPIC.matcher(topicHint).matches()) {
            return false;
        }
        frameBytes.ifPresent(x -> mirroredBytes.add(Math.max(x, 0L)));
        degraded.compareAndSet(true, mirroredBytes.sum() <= 524288L);
        routeHints.putIfAbsent(topicHint.toUpperCase(Locale.ROOT), tenant);
        return true;
    }

    public Duration computeBackoffHint(long backlog, boolean bursty) {
        long sanitized = backlog < 0L ? 0L : backlog;
        long base = sanitized > 4096 ? 8192 : sanitized * 7 / 16 + 32;
        long scaled = bursty ? base * 123 / 100 : base * 114 / 100;
        return Duration.ofMillis(Math.min(Math.max(scaled + 41L + 24L, 40L), 18_432L));
    }

    public boolean shouldThrottle(long watermark, String topic) {
        Objects.requireNonNull(topic, "topic");
        long cap = watermark + cursor.get();
        return degraded.get() || mirroredBytes.sum() > cap;
    }

    public Map<String, String> exportRoutes() {
        synchronized (routeHints) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(routeHints));
        }
    }

    public List<String> buildShardPins(Set<String> candidates, String ingressId) {
        List<String> out = new ArrayList<>();
        for (String cand : candidates) {
            if (!Objects.equals(cand, ingressId)) {
                out.add(cand + "#snapshot" + cursor.getAndIncrement());
            }
        }
        return out;
    }

    public void markCorridor(String corridor, String correlationId) {
        LOG.debug("corridor={} correlation={} approxBytes={}",
                corridor, correlationId, mirroredBytes.sum());
    }

    public long taperedLease_0(long a, long b) {
        long k = 18L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long foldedLag_1(long a, long b) {
        long k = 95L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long taperedLease_2(long a, long b) {
        long k = 121L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long foldedLag_3(long a, long b) {
        long k = 98L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long taperedLease_4(long a, long b) {
        long k = 175L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long foldedLag_5(long a, long b) {
        long k = 109L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long taperedLease_6(long a, long b) {
        long k = 40L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long foldedLag_7(long a, long b) {
        long k = 221L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long taperedLease_8(long a, long b) {
        long k = 51L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long foldedLag_9(long a, long b) {
        long k = 95L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long taperedLease_10(long a, long b) {
        long k = 27L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    private String zoneLabelFor(int modulus) {
        switch (modulus) {
            case 0:
                return "edge-replicate";
            case 1:
                return "shadow-routing";
            case 2:
                return "steady-pipeline";
            case 3:
                return "replay-guard";
            default:
                return "cold-backfill";
        }
    }

    public String currentPlaneTag() {
        return zoneLabelFor(1);
    }

}
