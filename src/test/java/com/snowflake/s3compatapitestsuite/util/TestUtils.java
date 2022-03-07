/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.s3compatapitestsuite.util;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtils {
    /**
     * Expect exception from function call
     * @param cbt The function call that would throw exception
     * @param expectedStatusCode Expected status code from the exception
     * @param expectedErrorCode Expected error code from the exception
     * @param expectedMsg Expected error message.
     * @throws Exception
     */
    public static void functionCallThrowsException(CodeBlockThrows cbt, Integer expectedStatusCode, String expectedErrorCode, String expectedMsg) throws Exception {
        try {
            cbt.invoke();
        } catch (AmazonS3Exception ex) {
            Assertions.assertEquals(expectedStatusCode, ex.getStatusCode());
            Assertions.assertEquals(expectedErrorCode, ex.getErrorCode());
            Assertions.assertNotNull(ex.getRequestId());
            // error message does not need to exactly like aws response
            // Assertions.assertEquals(expectedMsg, ex.getErrorMessage());
            return;
        } catch (Exception ex) {
            Assertions.fail("Expected an exception: " + ex);
        }
        // Should be un-reachable
        throw new Exception("Unreachable code reached");
    }

    public static File generateFileWithSize(String fileName, long size) {
        try {
            File file = new File(fileName);
            file.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            raf.close();
            return file;
        } catch (IOException e) {
            throw new RuntimeException("Generate File IOException: " + e);
        }
    }

    /**
     * Helper to obtain a random int value between specified bounds.
     *
     * @param lowerBound The minimum value to return (inclusive).
     * @param upperBound The maximum value to return (exclusive).
     * @return A random int within the specified range.
     */
    public static int getRandomInt(int lowerBound, int upperBound) {
        return ThreadLocalRandom.current().nextInt(lowerBound, upperBound);
    }

    /**
     * Operations tested.
     */
    public enum OPERATIONS {
        GET_BUCKET_LOCATION,
        GET_OBJECT,
        GET_OBJECT_METADATA,
        PUT_OBJECT,
        LIST_OBJECTS,
        LIST_OBJECTS_V2,
        LIST_VERSIONS,
        DELETE_OBJECT,
        DELETE_OBJECTS,
        COPY_OBJECT,
        SET_REGION,
        GENERATE_PRESIGNED_URL;
    }
}
