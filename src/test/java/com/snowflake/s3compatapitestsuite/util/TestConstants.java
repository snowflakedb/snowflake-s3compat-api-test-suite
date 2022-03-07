/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.s3compatapitestsuite.util;

public class TestConstants {
    /** A non-existing bucket. */
    public static final String NOT_EXISTING_BUCKET = "sf-non-existing-bucket";
    /** A prefix under region {@value  BUCKET_AT_REGION_1} */
    public static final String PREFIX_FOR_BUCKET_AT_REG_1 = "test-suite/ops";
    /** A local file for testing.*/
    public static final String LOCAL_FILE_PATH_1 = "src/resources/test1.txt";
    /** A second local file for testing. */
    public static final String LOCAL_FILE_PATH_2 = "src/resources/test2.json";
    /** A third file name for testing. */
    public static final String LARGE_FILE_NAME = "src/resources/largeFile.txt";
    /** A prefix under under bucket {@value  BUCKET_AT_REGION_1}*/
    public static final String PERFSTAT_PREFIX = "test-suite/perfstat";
    /** A local file for collecting perf stat.*/
    public static final String PERF_REPORT = "src/resources/perf_report.txt";
    /**
     * *******************************************************************
     * *********** Please fill in below values for testing. **************
     * *******************************************************************
     */
    /** An endpoint the client will make requests to. eg: "s3.amazonaws.com" */
    public static final String ENDPOINT = "";
    /** A region that a bucket locates, and it should correspond to s3 region. eg: "us-east-1" */
    public static final String REGION_1 = "";
    /** Another region that is different from region {@value  REGION_1}. eg: "us-west-2" */
    public static final String REGION_2 = "";
    /** A bucket locates at region {@value  REGION_1}. This bucket should have bucket versioning enabled. */
    public static final String BUCKET_AT_REGION_1 = "";
    /** A bucket locates at region {@value  REGION_2}. The bucket corresponds to S3 bucket. */
    public static final String BUCKET_AT_REGION_2 = "";
    /** A bucket locates at region {@value  REGION_1} but the provided credentials do not have access to. */
    public static final String BUCKET_EXISTS_BUT_NOT_ACCESSIBLE = "";
    /** A public bucket, which means without providing credentials, the client can have access to it. */
    public static final String PUBLIC_BUCKET = "";
    /** A prefix under bucket {@value  BUCKET_AT_REGION_1} which should contain over 1000 files, this is for page listing tests. */
    public static final String PREFIX_FOR_PAGE_LISTING_AT_REG_1 = "";
    /** The total number of files under prefix {@value  PREFIX_FOR_PAGE_LISTING_AT_REG_1}, it must > 1000 */
    public static final int PAGE_LISTING_TOTAL_SIZE = 0;
}
