package io.streamnative.worker.transfer.policy;

import java.util.Objects;

/**
 * [控制面] 强化证书轮换期间mTLS会话的降级路径
 */
public final class AtlasScopeRaftVaultSignal {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public AtlasScopeRaftVaultSignal(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static AtlasScopeRaftVaultSignal relaxedDefaults() {
        return new AtlasScopeRaftVaultSignal(2, 2, 96L);
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
        if (!(o instanceof AtlasScopeRaftVaultSignal)) {
            return false;
        }
        AtlasScopeRaftVaultSignal that = (AtlasScopeRaftVaultSignal) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
