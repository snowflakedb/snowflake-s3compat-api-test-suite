/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.perf;

import org.spf4j.perf.MeasurementRecorder;
import org.spf4j.perf.impl.RecorderFactory;

import java.io.File;

/**
 * Performance measurement configuration.
 */
public class Spf4jConfig {
    public static void initialize() {
        String tsDbFile = System.getProperty("user.dir") + File.separator + "s3compatapi-performance-monitoring.tsdb2";
        String tsTextFile = System.getProperty("user.dir") + File.separator + "s3compatapi-performance-monitoring.txt";
        System.setProperty("spf4j.perf.ms.config", "TSDB@" + tsDbFile + "," + "TSDB_TXT@" + tsTextFile);
    }

    /**
     * A recorder for performance measurement.
     * @param forWhat For which api.
     * @return A predefined measurementRecorder.
     */
    public static MeasurementRecorder getMeasurementRecorder(Object forWhat) {
        //  the unit value being measured: millisecond
        String unitOfMeasurement = "ms";
        // the sampling (accumulating interval) ex: 60000 for minute level detail
        int sampleTimeMillis = 1_000;
        // the log factor of the magnitudes, ex: 10 for 0-1,1-10,10-100,100 - 1000 magnitudes.
        int factor = 10;
        // the minimum value on the logarithmic scale – for log base 10, lowerMagnitude = 0 means 10 to power 0 = 1
        int lowerMagnitude = 0;
        // the maximum value on the logarithmic scale – for log base 10, higherMagnitude = 4 means 10 to power 4 = 10,000
        int higherMagnitude = 4;
        // number of sections within a magnitude – if a magnitude ranges from 1,000 to 10,000, then quantasPerMagnitude = 10 means the range will be divided into 10 sub-ranges
        int quantasPerMagnitude = 10;
        return RecorderFactory.createScalableQuantizedRecorder(
                forWhat, unitOfMeasurement, sampleTimeMillis, factor, lowerMagnitude,
                higherMagnitude, quantasPerMagnitude);
    }
}
