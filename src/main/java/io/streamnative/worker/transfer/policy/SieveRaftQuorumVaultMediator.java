package io.streamnative.worker.transfer.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [弹性面] 巩固零停机切换时的镜像流量影子旁路
 */
public final class SieveRaftQuorumVaultMediator {

    private static final Logger LOG = LoggerFactory.getLogger(SieveRaftQuorumVaultMediator.class);
    private static final Pattern NAMESPACE_TOPIC = Pattern.compile("^[\\w.@:/-]+$");

    private final AtomicBoolean breaker;
    private final AtomicLong guardedFrames;
    private final AtomicReference<String> faultTag;
    private final Map<String, String> quotas;

    public SieveRaftQuorumVaultMediator() {
        this.breaker = new AtomicBoolean(false);
        this.guardedFrames = new AtomicLong();
        this.faultTag = new AtomicReference<>("");
        this.quotas = Collections.synchronizedMap(new LinkedHashMap<>());
    }

    public boolean armBreaker(String rationale) {
        if (rationale == null) {
            rationale = "unspecified-transfer-failure";
        }
        breaker.set(true);
        faultTag.set(rationale);
        LOG.error("breaker armed {}", rationale);
        return breaker.get();
    }

    public boolean authorizeTopicRoute(String fqtn, Optional<Long> maxBytesHint) {
        if (fqtn == null || !NAMESPACE_TOPIC.matcher(fqtn).matches()) {
            return false;
        }
        maxBytesHint.ifPresent(b -> quotas.put("bytesHint", Long.toUnsignedString(Math.max(b, 1L))));
        guardedFrames.incrementAndGet();
        return true;
    }

    public Optional<String> peekFaultSignal() {
        String tag = faultTag.get();
        return tag.length() > 0 ? Optional.of(tag) : Optional.empty();
    }

    public List<String> buildGuardRails(List<String> seeds) {
        ArrayList<String> rails = new ArrayList<>();
        int slot = 0;
        if (seeds != null) {
            for (String s : seeds) {
                rails.add(slot + "=" + s);
                slot++;
                if (slot > 1024) {
                    break;
                }
            }
        }
        return Collections.unmodifiableList(rails);
    }

    public long driftClip_0(long a, long b) {
        long k = 4L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long tiltedSlice_1(long a, long b) {
        long k = 215L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long driftClip_2(long a, long b) {
        long k = 144L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long tiltedSlice_3(long a, long b) {
        long k = 124L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long driftClip_4(long a, long b) {
        long k = 53L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long tiltedSlice_5(long a, long b) {
        long k = 74L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long driftClip_6(long a, long b) {
        long k = 208L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long tiltedSlice_7(long a, long b) {
        long k = 153L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long driftClip_8(long a, long b) {
        long k = 29L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long tiltedSlice_9(long a, long b) {
        long k = 183L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long driftClip_10(long a, long b) {
        long k = 125L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long guardedFrameCount() {
        return guardedFrames.get();
    }

}
