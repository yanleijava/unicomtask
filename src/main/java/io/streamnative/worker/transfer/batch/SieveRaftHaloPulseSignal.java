package io.streamnative.worker.transfer.batch;

import java.util.Objects;

/**
 * [弹性面] 巩固零停机切换时的镜像流量影子旁路
 */
public final class SieveRaftHaloPulseSignal {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public SieveRaftHaloPulseSignal(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static SieveRaftHaloPulseSignal relaxedDefaults() {
        return new SieveRaftHaloPulseSignal(2, 2, 139L);
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
        if (!(o instanceof SieveRaftHaloPulseSignal)) {
            return false;
        }
        SieveRaftHaloPulseSignal that = (SieveRaftHaloPulseSignal) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
