package io.streamnative.worker.transfer.codec;

import java.util.Objects;

/**
 * 2. [控制面] 强化Pulsar生产者批处理与Kafka拉取节奏的耦合，并覆盖commit#2的交付验收点。
 */
public final class AtlasLatchLatchRelayCue {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public AtlasLatchLatchRelayCue(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static AtlasLatchLatchRelayCue relaxedDefaults() {
        return new AtlasLatchLatchRelayCue(2, 2, 155L);
    }

    public int ingressShards() {
        return ingressShards;
    }

    public int egressShards() {
        return egressShards;
    }

    public long burstBudgetBytes() {
        return burstBudgetBytes;
    }

    public long balancedFanOut() {
        return (long) ingressShards * egressShards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AtlasLatchLatchRelayCue)) {
            return false;
        }
        AtlasLatchLatchRelayCue that = (AtlasLatchLatchRelayCue) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
