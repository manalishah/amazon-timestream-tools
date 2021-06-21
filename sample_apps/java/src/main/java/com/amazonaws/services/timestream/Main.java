package com.amazonaws.services.timestream;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClient;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;

public class Main {
    public static final String DATABASE_NAME = "benchmark";
    public static final String TABLE_NAME = "java";
    public static final int DEFAULT_THREADS = 500;

    public static void main(String[] args) throws IOException {
        InputArguments inputArguments = parseArguments(args);
        AmazonTimestreamWrite writeClient = buildWriteClient();
        final AmazonTimestreamQuery queryClient = buildQueryClient();
        int threadCount = inputArguments.threadCount != 0 ? inputArguments.threadCount : DEFAULT_THREADS;
        int durationInMinutes = inputArguments.durationInMnutes;

        CrudAndSimpleIngestionExample crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient);
        CsvIngestionExample csvIngestionExample = new CsvIngestionExample(writeClient, threadCount, durationInMinutes);
        QueryExample queryExample = new QueryExample(queryClient);

        crudAndSimpleIngestionExample.createDatabase();
        crudAndSimpleIngestionExample.describeDatabase();
        if (inputArguments.kmsId != null) {
            crudAndSimpleIngestionExample.updateDatabase(inputArguments.kmsId);
            crudAndSimpleIngestionExample.describeDatabase();
        }
        crudAndSimpleIngestionExample.createTable();

        if (inputArguments.inputFile != null) {
            // Bulk record ingestion for bootstrapping a table with fresh data
            csvIngestionExample.bulkWriteRecords(inputArguments.inputFile);
        }

        // Query samples
//        queryExample.runAllQueries();

        // Try cancelling a query
//        queryExample.cancelQuery();

        // Run a query with Multiple pages
//        queryExample.runQueryWithMultiplePages(20000);

        // Cleanup commented out
         crudAndSimpleIngestionExample.deleteTable();
        // crudAndSimpleIngestionExample.deleteDatabase();

        System.exit(0);
    }


    private static InputArguments parseArguments(String[] args) {
        InputArguments inputArguments = new InputArguments();
        final CmdLineParser parser = new CmdLineParser(inputArguments);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }

        return inputArguments;
    }

    /**
     * Recommended Timestream write client SDK configuration:
     *  - Set SDK retry count to 10.
     *  - Use SDK DEFAULT_BACKOFF_STRATEGY
     *  - Set RequestTimeout to 20 seconds .
     *  - Set max connections to 5000 or higher.
     */
    private static AmazonTimestreamWrite buildWriteClient() {
        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        return AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion("us-west-2")
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    private static AmazonTimestreamQuery buildQueryClient() {
        AmazonTimestreamQuery client = AmazonTimestreamQueryClient.builder().withRegion("us-east-1").build();
        return client;
    }

}

