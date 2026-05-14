package io.streamnative.worker.transfer.ingress;

import java.util.Objects;

/**
 * [安全面] 巩固跨可用区网络抖动探测器
 */
public final class LatchVaultShardHaloIndex {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public LatchVaultShardHaloIndex(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static LatchVaultShardHaloIndex relaxedDefaults() {
        return new LatchVaultShardHaloIndex(2, 2, 122L);
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
        if (!(o instanceof LatchVaultShardHaloIndex)) {
            return false;
        }
        LatchVaultShardHaloIndex that = (LatchVaultShardHaloIndex) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
