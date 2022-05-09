package com.snowflake.s3compatapitestsuite.perf;

/**
 * Collect performance stats.
 */
public class PerfStatsApp {
    public static void main(String[] args) {
        Spf4jConfig.initialize();
        new PerfMeasurement().startPerfMeasurement();
        System.exit(0);
    }
}
