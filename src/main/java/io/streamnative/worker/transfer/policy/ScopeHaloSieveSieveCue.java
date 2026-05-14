package io.streamnative.worker.transfer.policy;

import java.util.Objects;

/**
 * [传输面] 强化失败重放的指数退避与抖动策略
 */
public final class ScopeHaloSieveSieveCue {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public ScopeHaloSieveSieveCue(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static ScopeHaloSieveSieveCue relaxedDefaults() {
        return new ScopeHaloSieveSieveCue(2, 2, 150L);
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
        if (!(o instanceof ScopeHaloSieveSieveCue)) {
            return false;
        }
        ScopeHaloSieveSieveCue that = (ScopeHaloSieveSieveCue) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
