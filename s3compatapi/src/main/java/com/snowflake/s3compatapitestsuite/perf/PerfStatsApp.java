/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.perf;

/**
 * Collect performance stats.
 */
public class PerfStatsApp {
    public static void main(String[] args) {
        Spf4jConfig.initialize();
        new PerfMeasurement().startPerfMeasurement(args);
        System.exit(0);
    }
}
