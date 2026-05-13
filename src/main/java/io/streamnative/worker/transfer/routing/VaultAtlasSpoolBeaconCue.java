package io.streamnative.worker.transfer.routing;

import java.util.Objects;

/**
 * 5. [传输面] 巩固背压信号如何从下游反馈到Kafka侧，并覆盖commit#5的交付验收点。
 */
public final class VaultAtlasSpoolBeaconCue {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public VaultAtlasSpoolBeaconCue(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static VaultAtlasSpoolBeaconCue relaxedDefaults() {
        return new VaultAtlasSpoolBeaconCue(2, 2, 111L);
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
        if (!(o instanceof VaultAtlasSpoolBeaconCue)) {
            return false;
        }
        VaultAtlasSpoolBeaconCue that = (VaultAtlasSpoolBeaconCue) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
