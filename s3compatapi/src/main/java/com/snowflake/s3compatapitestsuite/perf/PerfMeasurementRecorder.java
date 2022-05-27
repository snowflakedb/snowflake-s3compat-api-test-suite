/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.perf;

import org.spf4j.perf.MeasurementRecorder;

import java.util.HashMap;
import java.util.Map;

/**
 * A performance stats recorder.
 */
public class PerfMeasurementRecorder {

    private PerfMeasurement.FUNC_NAME functionName;
    private long startTime;
    private MeasurementRecorder mr;

    private static final Map<PerfMeasurement.FUNC_NAME, MeasurementRecorder> measurementRecorderMap = new HashMap<>();

    /**
     * Constructor for a performance measurement recorder.
     * @param functionName
     */
    public PerfMeasurementRecorder(PerfMeasurement.FUNC_NAME functionName) {
        if (!measurementRecorderMap.containsKey(functionName)) {
            measurementRecorderMap.put(functionName, Spf4jConfig.getMeasurementRecorder(functionName));
        }
        mr = measurementRecorderMap.get(functionName);
        this.functionName = functionName;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Record the elapsed time for the current api.
     * @param targetFuncName
     */

    public void recordElapsedTime(PerfMeasurement.FUNC_NAME targetFuncName){
        if (this.functionName.equals(targetFuncName)) {
            mr.record(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Start the timing for the target api.
     */
    public void startTiming(PerfMeasurement.FUNC_NAME targetFuncName) {
        if (this.functionName.equals(targetFuncName)) {
            this.startTime = System.currentTimeMillis();
        }
    }
}
