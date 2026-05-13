/**
 *
 */
package io.streamnative.common.stats;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import javax.management.MBeanServer;
import lombok.SneakyThrows;
import org.apache.pulsar.common.stats.JvmGCMetricsLogger;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.shade.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"checkstyle:JavadocType"})
public class JvmCMSGCMetricsLogger implements JvmGCMetricsLogger {

    private static final Logger log = LoggerFactory.getLogger(JvmCMSGCMetricsLogger.class);

    private volatile long accumulatedFullGcCount = 0;
    private volatile long currentFullGcCount = 0;
    private volatile long accumulatedFullGcTime = 0;
    private volatile long currentFullGcTime = 0;

    private static Object /*sun.management.HotspotRuntimeMBean*/ runtime;
    private static Method getTotalSafepointTimeHandle;
    private static Method getSafepointCountHandle;

    private Map<String, GCMetrics> gcMetricsMap = Maps.newHashMap();

    static {
        try {
            runtime = Class.forName("sun.management.ManagementFactoryHelper")
                    .getMethod("getHotspotRuntimeMBean")
                    .invoke(null);
            getTotalSafepointTimeHandle = runtime.getClass().getMethod("getTotalSafepointTime");
            getTotalSafepointTimeHandle.setAccessible(true);
            getSafepointCountHandle = runtime.getClass().getMethod("getSafepointCount");
            getSafepointCountHandle.setAccessible(true);

            // try to use the methods
            getTotalSafepointTimeHandle.invoke(runtime);
            getSafepointCountHandle.invoke(runtime);
        } catch (Throwable e) {
            log.warn("Failed to get Runtime bean", e);
        }
    }

    @SneakyThrows
    static long getTotalSafepointTime() {
        if (getTotalSafepointTimeHandle == null) {
            return -1;
        }
        return (long) getTotalSafepointTimeHandle.invoke(runtime);
    }

    @SneakyThrows
    static long getSafepointCount() {
        if (getTotalSafepointTimeHandle == null) {
            return -1;
        }
        return (long) getSafepointCountHandle.invoke(runtime);
    }

    /**
     * Metrics for the Garbage Collector.
     */
    static class GCMetrics {
        volatile long accumulatedGcCount = 0;
        volatile long currentGcCount = 0;
        volatile long accumulatedGcTime = 0;
        volatile long currentGcTime = 0;
    }

    @Override
    public void logMetrics(Metrics metrics) {
        metrics.put("jvm_full_gc_pause", currentFullGcTime);
        metrics.put("jvm_full_gc_count", currentFullGcCount);
        gcMetricsMap.forEach((name, metric) -> {
            metrics.put("jvm_" + name + "_gc_pause", metric.currentGcTime);
            metrics.put("jvm_" + name + "_gc_count", metric.currentGcCount);
            metrics.put("jvm_" + name + "_accumulated_gc_count", metric.accumulatedGcCount);
            metrics.put("jvm_" + name + "_accumulated_gc_pause", metric.accumulatedGcTime);
        });
    }

    @SuppressWarnings("restriction")
    @Override
    public void refresh() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        MBeanServer s = ManagementFactory.getPlatformMBeanServer();
        try {
            if (gcBeans != null) {
                for (GarbageCollectorMXBean gc : gcBeans) {
                    GCMetrics gcMetric = gcMetricsMap.computeIfAbsent(gc.getName(), gcName -> new GCMetrics());
                    long newGcTime = gc.getCollectionTime();
                    long newGcCount = gc.getCollectionCount();
                    gcMetric.currentGcCount = newGcCount - gcMetric.accumulatedGcCount;
                    gcMetric.currentGcTime = newGcTime - gcMetric.accumulatedGcTime;
                    gcMetric.accumulatedGcCount = newGcCount;
                    gcMetric.accumulatedGcTime = newGcTime;
                }
            }
            /**
             * Returns the accumulated time spent at safepoints in milliseconds. This is the accumulated elapsed time
             * that the application has been stopped for safepoint operations.
             * http://www.docjar.com/docs/api/sun/management/HotspotRuntimeMBean.html
             */
            long newSafePointTime = getTotalSafepointTime();
            long newSafePointCount = getSafepointCount();
            currentFullGcTime = newSafePointTime - accumulatedFullGcTime;
            currentFullGcCount = newSafePointCount - accumulatedFullGcCount;
            accumulatedFullGcTime = newSafePointTime;
            accumulatedFullGcCount = newSafePointCount;
        } catch (Exception e) {
            log.error("Failed to collect GC stats: {}", e.getMessage());
        }
    }

}
