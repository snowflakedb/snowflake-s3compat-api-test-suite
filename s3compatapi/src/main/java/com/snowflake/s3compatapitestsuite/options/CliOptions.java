/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.options;

import com.google.common.base.Strings;
import org.apache.commons.cli.*;

import java.util.Arrays;

/**
 * A class wrapping the parameters from command line arguments.
 */
public class CliOptions {
    /* A list of command line variables. */
    public static Options cliVariables = new Options();
    /* Formatter for output the variables with descriptions. */
    public static HelpFormatter formatter = new HelpFormatter();

    public CliOptions() {
        Arrays.stream(S3COMPAT_OPTIONS.values()).forEach(e -> cliVariables.addOption(e.getOption()));
    }

    /**
     * Get the value for a variable option.
     * @param option the variable option.
     * @return the value for the option.
     */
    public String getOptionValue(S3COMPAT_OPTIONS option) {
        String value = System.getProperty(option.getOptionName());
        if (!Strings.isNullOrEmpty(value)) {
             return value;
        }
        return "";
    }

    /**
     * Output the formatted options.
     */
    public void outputOptionsInfo() {
        formatter.printHelp("S3Compat API Test Parameters ", cliVariables);
    }

    /**
     * Enum for the variable options.
     */
    public enum S3COMPAT_OPTIONS {
        S3COMPAT_ACCESS_KEY(new Option("S3COMPAT_ACCESS_KEY", true, "Access key to used to authenticate the request.")),
        S3COMPAT_SECRET_KEY(new Option("S3COMPAT_SECRET_KEY", true, "Secret key to used to authenticate the request.")),
        END_POINT(new Option("END_POINT", true, "end point for the test suite")),
        BUCKET_1(new Option("BUCKET_NAME_1", true, "the first bucket name for testing, locate at region1")),
        REGION_1(new Option("REGION_1", true, "region for the first bucket")),
        REGION_2(new Option("REGION_2", true, "region for the second bucket")),
        BUCKET_EXISTS_BUT_NOT_ACCESSIBLE (new Option("NOT_ACCESSIBLE_BUCKET", true, "a bucket that is not accessible by the provided keys, in region1")),
        PREFIX_FOR_PAGE_LISTING(new Option("PREFIX_FOR_PAGE_LISTING", true, "the prefix for testing page listing")),
        PAGE_LISTING_TOTAL_SIZE(new Option("PAGE_LISTING_TOTAL_SIZE", true, "page listing total size"));

        private final Option op;
        S3COMPAT_OPTIONS(Option op) {
           this.op = op;
        }
        /* Get the options name. */
        public String getOptionName() {
            return this.op.getOpt();
        }
        /* Get the option for the variable enum. */
        public Option getOption() {
            return this.op;
        }
    }
}
