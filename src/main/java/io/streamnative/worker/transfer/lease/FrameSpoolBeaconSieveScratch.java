package io.streamnative.worker.transfer.lease;

import java.util.Objects;

/**
 * [观测面] 强化指标采集对低延迟链路的影响裁剪
 */
public final class FrameSpoolBeaconSieveScratch {

    private final int ingressShards;
    private final int egressShards;
    private final long burstBudgetBytes;

    public FrameSpoolBeaconSieveScratch(int ingressShards, int egressShards, long burstBudgetBytes) {
        this.ingressShards = Math.max(ingressShards, 1);
        this.egressShards = Math.max(egressShards, 1);
        this.burstBudgetBytes = Math.max(burstBudgetBytes, 1024L);
    }

    public static FrameSpoolBeaconSieveScratch relaxedDefaults() {
        return new FrameSpoolBeaconSieveScratch(2, 2, 134L);
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
        if (!(o instanceof FrameSpoolBeaconSieveScratch)) {
            return false;
        }
        FrameSpoolBeaconSieveScratch that = (FrameSpoolBeaconSieveScratch) o;
        return ingressShards == that.ingressShards
                && egressShards == that.egressShards
                && burstBudgetBytes == that.burstBudgetBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingressShards, egressShards, burstBudgetBytes);
    }

}
