package com.snowflake.s3compatapitestsuite.perf;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.google.common.base.Strings;
import com.snowflake.s3compatapitestsuite.EnvConstants;
import com.snowflake.s3compatapitestsuite.compatapi.DeleteRemoteObjectSpec;
import com.snowflake.s3compatapitestsuite.compatapi.S3CompatStorageClient;
import com.snowflake.s3compatapitestsuite.options.CliParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

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
        Option apisOpt = new Option("a", "APIs", true, "A list of APIs for measure performance");
        Option timesOpt = new Option("t", "times", true, "How many times to run each API");
        CommandLine cml = parseArgs(new Options().addOption(apisOpt).addOption(timesOpt), args);
        String funcNames = null;
        String times = null;
        if (cml != null && cml.hasOption(apisOpt)) {
            funcNames = cml.getOptionValue(apisOpt);
        }
        if (cml != null && cml.hasOption(timesOpt)) {
            times = cml.getOptionValue(timesOpt);
        }
        int timesInt = default_times;
        if (!Strings.isNullOrEmpty(times)) {
            timesInt = Integer.parseInt(times);
        }
        if (timesInt < 0) {
            throw new IllegalArgumentException("Number of times to run a API should be > 0");
        }
        try {
            if (!Strings.isNullOrEmpty(funcNames)) {
                String[] funcs = funcNames.split(",");
                for (String funcName : funcs) {
                    FUNC_NAME func = FUNC_NAME.lookupByName(funcName.trim());
                    if (func == null) {
                        throw new IllegalArgumentException(errorMessageForArguments(funcName));
                    }
                    measureOneFunc(func, timesInt);
                }
            } else {
                collectPerfStats(timesInt);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            tearDown();
        }
    }

    private CommandLine parseArgs(Options options, String[] args) {
        CliParser parser = new CliParser(options);
        return parser.parse(args);
    }

    private String errorMessageForArguments(String funcName) {
        StringBuilder sb = new StringBuilder();
        for (FUNC_NAME value : FUNC_NAME.values()) {
            sb.append(value.getName()).append(" ");
        }
        sb.append(". \n");
        sb.append("Example of CLI arguments: -a getObject,putObject -t 5");
        return String.format("Function name %s not supported. Supported arguments are: %s", funcName, sb);
    }

    private void setup() {
        EnvConstants.setUpParameterValues();
        bucketName =  EnvConstants.BUCKET_AT_REGION_1;
        credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(EnvConstants.ACCESS_KEY, EnvConstants.SECRET_KEY));
        clientWithRegion1 = new S3CompatStorageClient(credentialsProvider, EnvConstants.REGION_1, EnvConstants.ENDPOINT);
        // put files in order for testing
        putObjectResult1 = clientWithRegion1.putObject(bucketName, prefix + '/' + EnvConstants.LOCAL_FILE_PATH_1 , EnvConstants.LOCAL_FILE_PATH_1);
        putObjectResult2 = clientWithRegion1.putObject(bucketName, prefix + '/' + EnvConstants.LOCAL_FILE_PATH_2, EnvConstants.LOCAL_FILE_PATH_2);
        clientWithRegion1.setMeasurementPerformance(true);
    }

    private static void tearDown() {
        clientWithRegion1.setMeasurementPerformance(false);
        try {
            // cleanup all versions
            List<DeleteRemoteObjectSpec> toDeleteList = new ArrayList<>();
            for (S3VersionSummary v : clientWithRegion1.listVersions(bucketName, prefix, true /* useEncodeUrl*/, null /* maxKey */)) {
                toDeleteList.add(new DeleteRemoteObjectSpec(v.getKey(), v.getVersionId()));
                if (toDeleteList.size() == 1000) {
                    clientWithRegion1.deleteObjects(bucketName, toDeleteList);
                    toDeleteList.clear();
                }
            }
            clientWithRegion1.deleteObjects(bucketName, toDeleteList);
        } catch (Exception e) {
            System.out.println("PerfMeasure cleanup fail. " + e);
            e.printStackTrace();
        }
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
            case PUT_LARGE_SIZE_OBJECT:
                putLargeSizeObject();
                break;
            case LIST_OBJECTS_V2:
            case LIST_OBJECTS:
                measureListObjects(times);
                break;
            case LIST_LARGE_NUM_OBJECTS:
                measureListLargeNumObjects(times);
                break;
            case LIST_VERSIONS:
                measureListVersions(times);
                break;
            case DELETE_OBJECT:
                measureDeleteObject(times);
                break;
            case DELETE_OBJECTS:
                measureDeleteObjects(times /* numOfFilesToDelete */);
                break;
            case COPY_OBJECT:
                measureCopyObject(times);
                break;
            default:
                throw new IllegalArgumentException("Not Supported function " + func_name.getName());
        }
    }
    private void collectPerfStats(int times) {
        measureGetBucketLocation(times);
        measureGetObject(times);
        measureGetObjectMetadata(times);
        measurePutObject(times);
        measureCopyObject(times);
        measureListObjects(times);
        measureListLargeNumObjects(times);
       // putObjectWithLargeSize();
        measureListVersions(times);
        measureDeleteObject(times);
        measureDeleteObjects(times /* numOfFilesToDelete */);
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
        String remoteFilesName = "";
        for (int i = 0; i < times; i++) {
            remoteFilesName = prefix + '/' + EnvConstants.LOCAL_FILE_PATH_1 + "_" + i;
            clientWithRegion1.putObject(bucketName, remoteFilesName, EnvConstants.LOCAL_FILE_PATH_1);
        }
    }

    private void measureCopyObject(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.COPY_OBJECT);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.copyObject(bucketName, filePath1, putObjectResult1.getVersionId(), bucketName, "dst_" + filePath1 + i);
        }
    }

    private void measureListObjects(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.LIST_OBJECTS_V2);
        String testPrefix = prefix + "/listObjectsV2";
        uploadFilesForTesting(times, testPrefix);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.listObjectsV2(bucketName, testPrefix, null /* maxKeys */);
        }
    }
    private void measureListLargeNumObjects(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.LIST_LARGE_NUM_OBJECTS);
        String testPrefix = EnvConstants.PREFIX_FOR_PAGE_LISTING_AT_REG_1;
        for (int i = 0; i < times; i++) {
            clientWithRegion1.listObjectsV2(bucketName, testPrefix, null, FUNC_NAME.LIST_LARGE_NUM_OBJECTS);
        }
    }
    private void putLargeSizeObject() {
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
    private void measureListVersions(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.LIST_VERSIONS);
        String testPrefix = prefix + "/versions_" + getRandomInt(1, 5000);
        String remoteFileName = testPrefix + '/' + EnvConstants.LOCAL_FILE_PATH_1;
        PutObjectResult res = clientWithRegion1.putObject(bucketName, remoteFileName, EnvConstants.LOCAL_FILE_PATH_1);
        for (int i = 0; i < times; i++) {
            clientWithRegion1.listVersions(bucketName, remoteFileName, false, null);
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

    private void measureDeleteObject(int times) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.DELETE_OBJECT);
        int numFiles = times;
        String testPrefix = prefix + "/" + FUNC_NAME.DELETE_OBJECT.getName();
        List<PutObjectResult> putFilesToDelete = uploadFilesForTesting(numFiles, testPrefix);
        for (int i = 0; i < putFilesToDelete.size(); i++) {
            String fileName = "tempfile_" + i;
            clientWithRegion1.deleteObject(bucketName, testPrefix + '/' + fileName);
        }
    }

    private void measureDeleteObjects(int numFilesToDelete) {
        clientWithRegion1.setPerfMeasurement(FUNC_NAME.DELETE_OBJECTS);
        String testPrefix = prefix + "/" + FUNC_NAME.DELETE_OBJECTS.getName();
        List<PutObjectResult> putFilesForTests = uploadFilesForTesting(numFilesToDelete, testPrefix);
        List<DeleteRemoteObjectSpec> toDelete = new ArrayList<>();
        for (int i = 0; i < putFilesForTests.size(); i++) {
            PutObjectResult r = putFilesForTests.get(i);
            String fileName = "tempfile_" + i;
            toDelete.add(new DeleteRemoteObjectSpec(testPrefix + '/' + fileName, r.getVersionId()));
        }
        clientWithRegion1.deleteObjects(bucketName, toDelete);
    }
    private List<PutObjectResult> uploadFilesForTesting(int numFilesToUpload, String testPrefix) {
        clientWithRegion1.setMeasurementPerformance(false);
        List<PutObjectResult> res = new ArrayList<>();
        for (int i = 0; i < numFilesToUpload; i++) {
            String remoteFileName = testPrefix + "/tempfile_" + i;
            res.add(clientWithRegion1.putObject(bucketName,remoteFileName,  EnvConstants.LOCAL_FILE_PATH_1));
        }
        clientWithRegion1.setMeasurementPerformance(true);
        return res;
    }

    public enum FUNC_NAME {
        GET_BUCKET_LOCATION("getBucketLocation"),
        GET_OBJECT("getObject"),
        GET_OBJECT_METADATA("getObjectMetadata"),
        PUT_OBJECT("putObject"),
        PUT_LARGE_SIZE_OBJECT("putLargeSizeObject"),
        LIST_OBJECTS("listObject"),
        LIST_OBJECTS_V2("listObjectV2"),
        LIST_LARGE_NUM_OBJECTS("listLargeNumObjects"),
        LIST_VERSIONS("listVersions"),
        DELETE_OBJECT("deleteObject"),
        DELETE_OBJECTS("deleteObjects"),
        COPY_OBJECT("copyObject");

        private final String name;

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
