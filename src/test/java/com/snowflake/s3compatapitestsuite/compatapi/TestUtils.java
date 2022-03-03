package com.snowflake.s3compatapitestsuite.compatapi;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.junit.jupiter.api.Assertions;

public class TestUtils {
    /**
     * Expect exception from function call
     * @param cbt The function call that would throw exception
     * @param expectedStatusCode Expected status code from the exception
     * @param expectedErrorCode Expected error code from the exception
     * @param expectedRegionFromExceptionMsg Expected region name from the exception.
     * @throws Exception
     */
    public static void functionCallThrowsException(CodeBlockThrows cbt, Integer expectedStatusCode, String expectedErrorCode, String expectedRegionFromExceptionMsg) throws Exception {
        try {
            cbt.invoke();
        } catch (AmazonS3Exception ex) {
            if (ex instanceof MultiObjectDeleteException) {
                ex.getErrorMessage().equals("One or more objects could not be deleted");
            }
            Assertions.assertEquals(ex.getStatusCode(), expectedStatusCode);
            Assertions.assertEquals(ex.getErrorCode(), expectedErrorCode);
            Assertions.assertNotNull(ex.getRequestId());
            if (expectedRegionFromExceptionMsg != null) {
                Assertions.assertEquals(expectedRegionFromExceptionMsg, ex.getAdditionalDetails().get("Region"));
            }
            return;
        } catch (Exception ex) {
            Assertions.fail("Expected an exception: " + ex);
        }
        // Should be un-reachable
        throw new Exception("Unreachable code reached");
    }


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
