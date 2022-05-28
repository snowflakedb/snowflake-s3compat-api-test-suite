/**
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
 */
package com.snowflake.s3compatapitestsuite.util;

/**
 * Interface for code block to throw exception.
 */
public interface CodeBlockThrows {
    /**
     * The code block to invoke to attempt to induce a particular exception.
     *
     * @throws Exception
     */
    void invoke() throws Exception;
}
