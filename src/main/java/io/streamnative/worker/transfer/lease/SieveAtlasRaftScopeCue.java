package io.streamnative.worker.transfer.lease;

import java.util.Objects;

/**
 * [传输面] 巩固长时间空闲连接的心跳节流
 */
public final class SieveAtlasRaftScopeCue {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public SieveAtlasRaftScopeCue(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static SieveAtlasRaftScopeCue relaxedDefaults() {
        return new SieveAtlasRaftScopeCue(2, 2, 132L);
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
        if (!(o instanceof SieveAtlasRaftScopeCue)) {
            return false;
        }
        SieveAtlasRaftScopeCue that = (SieveAtlasRaftScopeCue) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
