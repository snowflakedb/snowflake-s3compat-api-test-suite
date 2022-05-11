package com.snowflake.s3compatapitestsuite.perf;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.common.base.Strings;
import com.snowflake.s3compatapitestsuite.EnvConstants;
import com.snowflake.s3compatapitestsuite.compatapi.DeleteRemoteObjectSpec;
import com.snowflake.s3compatapitestsuite.compatapi.S3CompatStorageClient;
import org.spf4j.perf.MeasurementRecorder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A Class wrapping up performance measurement.
 */
public class PerfMeasurement {
    /** A client created with region TestConstants.region1 provided. */
    private static S3CompatStorageClient clientWithRegion1;
    /** A prefix for running tests. */
    private static final String prefix = EnvConstants.PERFSTAT_PREFIX;
    private static String bucketName ;
    private static String filePath1 = prefix + "/" + EnvConstants.LOCAL_FILE_PATH_1;
    private static AWSCredentialsProvider credentialsProvider = null;

    private static final int default_times = 20;
    private static PutObjectResult putObjectResult1;
    private static PutObjectResult putObjectResult2;

    /**
     * Constructor for performance measurement.
     */
    public PerfMeasurement() {
        setup();
    }

    /**
     * Start to collect performance stats.
     * @param args Execution arguments passed from the CLI.
     *             eg:
     */
    public void startPerfMeasurement(String[] args) {
        if (args.length > 2) {
            throw new IllegalArgumentException("Only accept one or two arguments, " +
                    "format: -Dexec.args=\"getObject,putObject 5\" , first argument is a list of APIs separated by ',', " +
                    "second argument is for times to run the API. The second argument is optional.");
        }
        String funcNames = null;
        String times = null;
        if (args.length > 0) {
            funcNames = args[0];
        }
        if (args.length > 1) {
            times = args[1];
        }
        int timesInt = default_times;
        if (!Strings.isNullOrEmpty(times)) {
            timesInt = Integer.parseInt(times);
        }
        if (timesInt < 0) {
            throw new IllegalArgumentException("Number of times to run a function should be > 0");
        }
        if (!Strings.isNullOrEmpty(funcNames)) {
            String[] funcs = funcNames.split(",");
            for (String funcName: funcs) {
                FUNC_NAME func = FUNC_NAME.lookupByName(funcName.trim());
                if (func == null) {
                    throw new IllegalArgumentException(errorMessageForArguments(funcName));
                }
                measureOneFunc(func, timesInt);
            }
        } else {
            collectPerfStats(timesInt);
        }
        tearDown();
    }

    private String errorMessageForArguments(String funcName) {
        StringBuilder sb = new StringBuilder();
        for (FUNC_NAME value : FUNC_NAME.values()) {
            sb.append(value.getName()).append(" ");
        }
        sb.append(". \n");
        sb.append("Example of CLI arguments: -Dexec.args=\"getObject,putObject 5\"");
        return String.format("Function name %s not supported. Supported arguments are: %s", funcName, sb);
    }

    private void setup() {
        EnvConstants.setUpParameterValues();
        bucketName =  EnvConstants.BUCKET_AT_REGION_1;
        credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(EnvConstants.ACCESS_KEY, EnvConstants.SECRET_KEY));
        clientWithRegion1 = new S3CompatStorageClient(credentialsProvider, EnvConstants.REGION_1, EnvConstants.ENDPOINT);
        clientWithRegion1.setMeasurementPerfomance(true);
        // put a file in order for testing
        putObjectResult1 = clientWithRegion1.putObject(bucketName, prefix, EnvConstants.LOCAL_FILE_PATH_1);
        putObjectResult2 = clientWithRegion1.putObject(bucketName, prefix, EnvConstants.LOCAL_FILE_PATH_2);
    }

    private static void tearDown() {
        // Delete files that we uploaded to the prefix for testing.
        clientWithRegion1.deleteObjects(EnvConstants.BUCKET_AT_REGION_1, prefix);
    }

    private void measureOneFunc(FUNC_NAME func_name, int times) {
        switch (func_name) {
            case GET_BUCKET_LOCATION:
                measureGetBucketLocation(times);
                break;
            case GET_OBJECT:
                measureGetObject(times);
                break;
            case GET_OBJECT_METADATA:
                measureGetObjectMetadata(times);
                break;
            case PUT_OBJECT:
                measurePutObject(times);
                break;
            case LIST_OBJECTS:
                measureListObjects(times);
                break;
            case LIST_VERSIONS:
                listVersions();
                break;
            case DELETE_OBJECT:
                measureDeleteObject();
                break;
            case DELETE_OBJECTS:
                measureDeleteObjects();
                break;
            default:
                throw new IllegalArgumentException("Not Supported function " + func_name);
        }
    }
    private void collectPerfStats(int times) {
        measureGetBucketLocation(times);
        measureGetObject(times);
        measureGetObjectMetadata(times);
        measurePutObject(times);
        measureCopyObject(times);
        measureListObjects(times);
       // putObjectWithLargeSize();
        listLargeNumOfObjects();
        listVersions();
        measureDeleteObject();
        measureDeleteObjects();
    }
    private void measureGetBucketLocation(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.GET_BUCKET_LOCATION);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.getBucketLocation(bucketName);
        }
    }
    private void measureGetObjectMetadata(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.GET_OBJECT_METADATA);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.getObjectMetadata(bucketName, filePath1, putObjectResult1.getMetadata().getVersionId());
        }
    }
    private void measureGetObject(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.GET_OBJECT);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.getObject(bucketName, filePath1);
        }
    }
    private void measurePutObject(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.PUT_OBJECT);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.putObject(bucketName, prefix, EnvConstants.LOCAL_FILE_PATH_1);
        }
    }

    private void measureCopyObject(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.COPY_OBJECT);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.copyObject(bucketName, filePath1, putObjectResult1.getVersionId(), bucketName, "dst_" + filePath1);
        }
    }

    private void measureListObjects(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.LIST_OBJECTS_V2);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.listObjectsV2(bucketName, prefix, null /* maxKeys */);
        }
    }
    private void putObjectWithLargeSize() {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.PUT_OBJECT);
        // pubObject -- size of 5GB
        File file = generateFileWithSize(EnvConstants.LARGE_FILE_NAME, 5368709120L); // 5GB
        clientWithRegion1.putObject(bucketName, prefix, EnvConstants.LARGE_FILE_NAME);
        file.delete();
    }
    private static File generateFileWithSize(String fileName, long size) {
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
    private void listLargeNumOfObjects() {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.LIST_OBJECTS_V2);
        clientWithRegion1.listObjectsV2(bucketName, EnvConstants.PREFIX_FOR_PAGE_LISTING_AT_REG_1, null /* maxKeys*/);
    }
    private void listVersions() {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.LIST_VERSIONS);
        String testPrefix = prefix + "/versions" + getRandomInt(1, 5000);
        PutObjectResult res = clientWithRegion1.putObject(bucketName, testPrefix, EnvConstants.LOCAL_FILE_PATH_1);
        for (int i = 0; i < default_times; i++) {
            clientWithRegion1.listVersions(bucketName, testPrefix + "/" + EnvConstants.LOCAL_FILE_PATH_1, false);
        }
    }
    /**
     * Helper to obtain a random int value between specified bounds.
     *
     * @param lowerBound The minimum value to return (inclusive).
     * @param upperBound The maximum value to return (exclusive).
     * @return A random int within the specified range.
     */
    private static int getRandomInt(int lowerBound, int upperBound) {
        return ThreadLocalRandom.current().nextInt(lowerBound, upperBound);
    }

    private void measureDeleteObject() {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.DELETE_OBJECT);
        int numFiles = getRandomInt(1, 20);
        String testPrefix = prefix + "/deleteFiles";
        String fileNamePrefix = "src/main/resources/";
        for (int i = 0; i < numFiles; i++) {
            String fileName = fileNamePrefix + "tempfile_" + i;
            File file = generateFileWithSize(fileName, 50);
            clientWithRegion1.putObject(bucketName, testPrefix, fileName);
            file.delete();
        }
        for (int i = 0; i < numFiles; i++) {
            String fileName = fileNamePrefix + "tempfile_" + i;
            clientWithRegion1.deleteObject(bucketName, fileName);
        }
    }

    private void measureDeleteObjects() {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.DELETE_OBJECTS);
        String testPrefix = prefix + "/deleteFiles";
        String fileNamePrefix = "src/main/resources/";
        List<DeleteRemoteObjectSpec> toDelete = new ArrayList<>();
        for (int i = 0; i < default_times; i++) {
            String fileName = fileNamePrefix + "tempfile_" + i;
            File file = generateFileWithSize(fileName, 50);
            PutObjectResult putRes = clientWithRegion1.putObject(bucketName, testPrefix, fileName);
            file.delete();
            toDelete.add(new DeleteRemoteObjectSpec(fileName, putRes.getVersionId()));
        }
        clientWithRegion1.deleteObjects(bucketName, toDelete);

    }

    public enum FUNC_NAME {
        GET_BUCKET_LOCATION("getBucketLocation"),
        GET_OBJECT("getObject"),
        GET_OBJECT_METADATA("getObjectMetadata"),
        PUT_OBJECT("putObject"),
        LIST_OBJECTS("listObject"),
        LIST_OBJECTS_V2("listObjectV2"),
        LIST_VERSIONS("listVersions"),
        DELETE_OBJECT("deleteObject"),
        DELETE_OBJECTS("deleteObjects"),
        COPY_OBJECT("copyObject"),
        SET_REGION("setRegion"),
        GENERATE_PRESIGNED_URL("generatePresignedUrl");

        private String name;

        FUNC_NAME(String name) {
            this.name = name;
        }
        public static FUNC_NAME lookupByName (String func) {
            if (func == null) {
                return null;
            }
            for (FUNC_NAME func_name : FUNC_NAME.values()) {
                if (func_name.getName().equalsIgnoreCase(func)) {
                    return func_name;
                }
            }
            return null;
        }
        public String getName() {
            return this.name;
        }
    }
}
