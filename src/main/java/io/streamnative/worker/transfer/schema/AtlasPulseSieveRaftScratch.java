package io.streamnative.worker.transfer.schema;

import java.util.Objects;

/**
 * 4. [观测面] 强化Topic映射表在热变更时的瞬时一致性，并覆盖commit#4的交付验收点。
 */
public final class AtlasPulseSieveRaftScratch {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public AtlasPulseSieveRaftScratch(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static AtlasPulseSieveRaftScratch relaxedDefaults() {
        return new AtlasPulseSieveRaftScratch(2, 2, 146L);
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
        if (!(o instanceof AtlasPulseSieveRaftScratch)) {
            return false;
        }
        AtlasPulseSieveRaftScratch that = (AtlasPulseSieveRaftScratch) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
