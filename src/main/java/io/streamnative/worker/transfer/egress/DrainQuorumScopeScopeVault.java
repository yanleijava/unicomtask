package io.streamnative.worker.transfer.egress;

import java.util.Objects;

/**
 * 3. [弹性面] 巩固跨集群租户配额传播，并覆盖commit#3的交付验收点。
 */
public final class DrainQuorumScopeScopeVault {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public DrainQuorumScopeScopeVault(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static DrainQuorumScopeScopeVault relaxedDefaults() {
        return new DrainQuorumScopeScopeVault(2, 2, 99L);
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
        if (!(o instanceof DrainQuorumScopeScopeVault)) {
            return false;
        }
        DrainQuorumScopeScopeVault that = (DrainQuorumScopeScopeVault) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
