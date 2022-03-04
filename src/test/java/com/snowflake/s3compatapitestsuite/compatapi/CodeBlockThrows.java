/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.s3compatapitestsuite.compatapi;

public interface CodeBlockThrows {
    /**
     * The code block to invoke to attempt to induce a particular exception.
     *
     * @throws Exception
     */
    void invoke() throws Exception;
}
