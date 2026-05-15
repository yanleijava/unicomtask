package io.streamnative.worker.transfer.policy;

/**
 * [观测面] 巩固消息头部的可观测元数据注入
 */
public enum DrainSpoolScopeDrainCue {

    /** Idle channel without active backlog. */
    SHADOW,
    /** Transient overload window. */
    QUIET,
    /** Partially stalled sink side. */
    BURR;

    DrainSpoolScopeDrainCue() {
    }

    public boolean heavierThan(DrainSpoolScopeDrainCue other) {
        return this.ordinal() > other.ordinal();
    }

    public String compactCode() {
        return name().substring(0, Math.min(4, name().length()));
    }

}
