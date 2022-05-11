package com.snowflake.s3compatapitestsuite;

import com.google.common.base.Strings;
import com.snowflake.s3compatapitestsuite.options.CliOptions;

import java.util.Map;

public class EnvConstants {
    /**
     * A non-existing bucket.
     */
    public static final String NOT_EXISTING_BUCKET = "sf-non-existing-bucket";
    /**
     * A prefix under region {@value  BUCKET_AT_REGION_1}
     */
    public static final String PREFIX_FOR_BUCKET_AT_REG_1 = "test-suite/ops";
    /**
     * A local file for testing.
     */
    public static final String LOCAL_FILE_PATH_1 = "src/main/resources/test1.txt";
    /**
     * A second local file for testing.
     */
    public static final String LOCAL_FILE_PATH_2 = "src/main/resources/test2.json";
    /**
     * A third file name for testing.
     */
    public static final String LARGE_FILE_NAME = "src/main/resources/largeFile.txt";

    /** A prefix under under bucket {@value  BUCKET_AT_REGION_1}*/
    public static final String PERFSTAT_PREFIX = "test-suite/perfstat";
    /**
     * *************************************************************************************************
     * *********** Below values need to be filled from environment variable or system properties . *****
     *    [variables]                    [description]
     *  REGION_1                   region for the first bucket
     *  REGION_2                   region for the second bucket
     *  BUCKET_NAME_1              the first bucket name for testing, locate at region1
     *  BUCKET_NAME_2              the second bucket name for testing, locate at region2
     *  END_POINT                  end point for the test suite
     *  NOT_ACCESSIBLE_BUCKET      a bucket that is not accessible by the provided keys, in region1
     *  PAGE_LISTING_TOTAL_SIZE    page listing total size
     *  PREFIX_FOR_PAGE_LISTING    the prefix for testing page listing
     *  S3COMPAT_ACCESS_KEY        Access key to used to authenticate the request.
     *  S3COMPAT_SECRET_KEY        Secret key to used to authenticate the request.
     * *************************************************************************************************
     */
    /**
     * An endpoint the client will make requests to. eg: "s3.amazonaws.com"
     */
    public static String ENDPOINT = "";
    /**
     * A region that a bucket locates, and it should correspond to s3 region. eg: "us-east-1"
     */
    public static String REGION_1 = "";
    /**
     * Another region that is different from region {@value  REGION_1}. eg: "us-west-2"
     */
    public static String REGION_2 = "";
    /**
     * A bucket locates at region {@value  REGION_1}. This bucket should have bucket versioning enabled.
     */
    public static String BUCKET_AT_REGION_1 = "";
    /**
     * Access key for {@value BUCKET_AT_REGION_1}
     */
    public static String ACCESS_KEY = "";
    /**
     * Secret key for {@value BUCKET_AT_REGION_1}
     */
    public static String SECRET_KEY = "";
    /**
     * A bucket locates at region {@value  REGION_2}. The bucket corresponds to S3 bucket.
     */
    public static String BUCKET_AT_REGION_2 = "";
    /**
     * A bucket locates at region {@value  REGION_1} but the provided credentials do not have access to.
     */
    public static String BUCKET_EXISTS_BUT_NOT_ACCESSIBLE = "";
    /**
     * A prefix under bucket {@value  BUCKET_AT_REGION_1} which should contain over 1000 files, this is for page listing tests.
     */
    public static String PREFIX_FOR_PAGE_LISTING_AT_REG_1 = "";
    /**
     * The total number of files under prefix {@value  PREFIX_FOR_PAGE_LISTING_AT_REG_1}, it must > 1000
     */
    public static int PAGE_LISTING_TOTAL_SIZE = 0;

    /**
     * Environment variables.
     */
    public static Map<String, String> env = System.getenv();

    /**
     * Read the necessary variables to enable tests.
     */
    public static void setUpParameterValues() {
        CliOptions options = new CliOptions();
        ENDPOINT = getConstantValue(CliOptions.S3COMPAT_OPTIONS.END_POINT, options);
        REGION_1 = getConstantValue(CliOptions.S3COMPAT_OPTIONS.REGION_1, options);
        REGION_2 = getConstantValue(CliOptions.S3COMPAT_OPTIONS.REGION_2, options);
        BUCKET_AT_REGION_1 = getConstantValue(CliOptions.S3COMPAT_OPTIONS.BUCKET_1, options);
        ACCESS_KEY = getConstantValue(CliOptions.S3COMPAT_OPTIONS.S3COMPAT_ACCESS_KEY, options);
        SECRET_KEY = getConstantValue(CliOptions.S3COMPAT_OPTIONS.S3COMPAT_SECRET_KEY, options);
        BUCKET_AT_REGION_2 = getConstantValue(CliOptions.S3COMPAT_OPTIONS.BUCKET_2, options);
        BUCKET_EXISTS_BUT_NOT_ACCESSIBLE = getConstantValue(CliOptions.S3COMPAT_OPTIONS.BUCKET_EXISTS_BUT_NOT_ACCESSIBLE, options);
        PREFIX_FOR_PAGE_LISTING_AT_REG_1 = getConstantValue(CliOptions.S3COMPAT_OPTIONS.PREFIX_FOR_PAGE_LISTING, options);
        PAGE_LISTING_TOTAL_SIZE = Integer.parseInt(getConstantValue(CliOptions.S3COMPAT_OPTIONS.PAGE_LISTING_TOTAL_SIZE, options));
    }

    /**
     * Get the value of the option for testing. Return the system variable, otherwise return the command line variable.
     *
     * @param opt        The option name.
     * @param cliOptions Command line options
     * @return The value for the option
     */
    public static String getConstantValue(CliOptions.S3COMPAT_OPTIONS opt, CliOptions cliOptions) {
        String optionName = opt.getOptionName();
        String value;
        if (env.containsKey(optionName) && env.get(optionName) != null) {
            value = env.get(optionName);
        } else {
            value = cliOptions.getOptionValue(opt);
        }
        if (Strings.isNullOrEmpty(value)) {
            cliOptions.outputOptionsInfo();
            throw new RuntimeException("Please provide environment variable " + optionName);
        }
        return value;
    }
}
