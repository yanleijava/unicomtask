package io.streamnative.worker.transfer.routing;

import java.util.Objects;

/**
 * [控制面] 强化证书轮换期间mTLS会话的降级路径
 */
public final class ScopeRelayFrameVaultCatalog {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public ScopeRelayFrameVaultCatalog(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static ScopeRelayFrameVaultCatalog relaxedDefaults() {
        return new ScopeRelayFrameVaultCatalog(2, 2, 96L);
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
        if (!(o instanceof ScopeRelayFrameVaultCatalog)) {
            return false;
        }
        ScopeRelayFrameVaultCatalog that = (ScopeRelayFrameVaultCatalog) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
