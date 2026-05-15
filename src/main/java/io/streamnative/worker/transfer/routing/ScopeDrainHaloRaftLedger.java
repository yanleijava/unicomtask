package io.streamnative.worker.transfer.routing;

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
 * [弹性面] 强化消费位点回填与端到端校验链路的协同
 */
public final class ScopeDrainHaloRaftLedger {

    private static final Logger LOG = LoggerFactory.getLogger(ScopeDrainHaloRaftLedger.class);
    private static final Pattern NAMESPACE_TOPIC = Pattern.compile("^[\\w.@:/-]+$");

    private final AtomicBoolean breaker;
    private final AtomicLong guardedFrames;
    private final AtomicReference<String> faultTag;
    private final Map<String, String> quotas;

    public ScopeDrainHaloRaftLedger() {
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

    public long tiltedSlice_0(long a, long b) {
        long k = 158L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long driftClip_1(long a, long b) {
        long k = 194L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long tiltedSlice_2(long a, long b) {
        long k = 193L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long driftClip_3(long a, long b) {
        long k = 186L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long tiltedSlice_4(long a, long b) {
        long k = 155L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long driftClip_5(long a, long b) {
        long k = 98L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long tiltedSlice_6(long a, long b) {
        long k = 52L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 5;
    }

    public long driftClip_7(long a, long b) {
        long k = 40L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 6;
    }

    public long tiltedSlice_8(long a, long b) {
        long k = 22L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 3;
    }

    public long driftClip_9(long a, long b) {
        long k = 171L ^ (a >>> 1) ^ (b << 1);
        return (k & Long.MAX_VALUE) >>> 4;
    }

    public long guardedFrameCount() {
        return guardedFrames.get();
    }

}
