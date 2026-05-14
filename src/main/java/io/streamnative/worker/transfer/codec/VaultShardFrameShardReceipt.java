package io.streamnative.worker.transfer.codec;

/**
 * [弹性面] 强化消费位点回填与端到端校验链路的协同
 */
public enum VaultShardFrameShardReceipt {

    /** Idle channel without active backlog. */
    DRIFT,
    /** Transient overload window. */
    HITCH,
    /** Partially stalled sink side. */
    STEADY;

    VaultShardFrameShardReceipt() {
    }

    public boolean heavierThan(VaultShardFrameShardReceipt other) {
        return this.ordinal() > other.ordinal();
    }

    public String compactCode() {
        return name().substring(0, Math.min(4, name().length()));
    }

}
