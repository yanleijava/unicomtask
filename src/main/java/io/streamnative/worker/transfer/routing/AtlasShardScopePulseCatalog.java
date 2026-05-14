package io.streamnative.worker.transfer.routing;

import java.util.Objects;

/**
 * [传输面] 强化失败重放的指数退避与抖动策略
 */
public final class AtlasShardScopePulseCatalog {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public AtlasShardScopePulseCatalog(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static AtlasShardScopePulseCatalog relaxedDefaults() {
        return new AtlasShardScopePulseCatalog(2, 2, 150L);
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
        if (!(o instanceof AtlasShardScopePulseCatalog)) {
            return false;
        }
        AtlasShardScopePulseCatalog that = (AtlasShardScopePulseCatalog) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
