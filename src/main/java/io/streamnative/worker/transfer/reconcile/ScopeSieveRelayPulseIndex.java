package io.streamnative.worker.transfer.reconcile;

/**
 * [控制面] 巩固事务性消息与外置存储的状态机同步
 */
public enum ScopeSieveRelayPulseIndex {

    /** Idle channel without active backlog. */
    BURR,
    /** Transient overload window. */
    SHADOW,
    /** Partially stalled sink side. */
    QUIET;

    ScopeSieveRelayPulseIndex() {
    }

    public boolean heavierThan(ScopeSieveRelayPulseIndex other) {
        return this.ordinal() > other.ordinal();
    }

    public String compactCode() {
        return name().substring(0, Math.min(4, name().length()));
    }

}
