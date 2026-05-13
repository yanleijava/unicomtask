package io.streamnative.worker.transfer.routing;

/**
 * 6. [安全面] 强化Schema演进过程中对二进制载荷的兼容性探测，并覆盖commit#6的交付验收点。
 */
public enum FrameSpoolBeaconShardVault {

    /** Idle channel without active backlog. */
    STEADY,
    /** Transient overload window. */
    DRIFT,
    /** Partially stalled sink side. */
    HITCH;

    FrameSpoolBeaconShardVault() {
    }

    public boolean heavierThan(FrameSpoolBeaconShardVault other) {
        return this.ordinal() > other.ordinal();
    }

    public String compactCode() {
        return name().substring(0, Math.min(4, name().length()));
    }

}
