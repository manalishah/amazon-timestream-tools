package com.amazonaws.services.timestream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.AmazonTimestreamWriteException;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.RejectedRecord;
import com.amazonaws.services.timestreamwrite.model.RejectedRecordsException;
import com.amazonaws.services.timestreamwrite.model.TimeUnit;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;

public class CsvIngestionExample {

    private final AmazonTimestreamWrite amazonTimestreamWrite;
    private final ExecutorService executorService;
    private static final long KEEP_ALIVE_TIME_IN_MINUTES = 10;
    private long threadCount;

    private long ingested;
    private long rejected;

    public CsvIngestionExample(AmazonTimestreamWrite amazonTimestreamWrite, int threadCount) {
        this.amazonTimestreamWrite = amazonTimestreamWrite;
        this.threadCount = threadCount;
        this.executorService = new ThreadPoolExecutor(
            threadCount,
            threadCount,
            KEEP_ALIVE_TIME_IN_MINUTES,
            java.util.concurrent.TimeUnit.MINUTES,
            new LinkedBlockingDeque<>(threadCount * 5),
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void bulkWriteRecords(String csvFilePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
        List<Record> records = new ArrayList<>();

        try {

            int counter = 0;
            // Convert record batches to WR and submit them

            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                String[] columns = line.split(",");

                List<Dimension> dimensions = new ArrayList<>();
                dimensions.add(new Dimension().withName(columns[0]).withValue(columns[1]));
                dimensions.add(new Dimension().withName(columns[2]).withValue(columns[3]));
                dimensions.add(new Dimension().withName(columns[4]).withValue(columns[5]));

                // override the value on the file to get an ingestion that is around current time
                // Replicate a single line into 100 records.


                Record record = new Record()
                                    .withDimensions(dimensions)
                                    .withTimeUnit(TimeUnit.MILLISECONDS)
                                    .withMeasureName(columns[6])
                                    .withMeasureValue(columns[7])
                                    .withMeasureValueType(columns[8])
                                    .withTime(String.valueOf(Instant.now().toEpochMilli() - counter * 50L) );
                records.add(record);
                counter++;

                if (counter == threadCount * 100) {
                    break;
                }
            }
            System.out.printf("Completed parsing records [%d]. \n", counter);

        } finally {
            reader.close();
        }
        for ( int i = 0; i< 10; i++) {
            // Submit records using multiple threads
            records = records.stream().peek(record -> {record.setTime(String.valueOf(Instant.now().toEpochMilli()));
            }).collect(Collectors.toList());
            submitBatch(records);
        }
    }

    private void submitBatch(List<Record> recordBatches) {
        Instant start = Instant.now();
        try {
            // Convert record batches to WR and submit them
            Map<Integer, Future<WriteRecordsResult>> futures = new HashMap<>(recordBatches.size());
            int count = 0;
            for (int i = 0; i< recordBatches.size(); i = i+ 100) {
                WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                                                              .withDatabaseName(DATABASE_NAME)
                                                              .withTableName(TABLE_NAME)
                    .withRecords(recordBatches.subList(i, Math.min(i + 100, recordBatches.size())));
                futures.put(i, write(writeRecordsRequest));
                count++;
                // Wait for WR requests to complete
                if (count % this.threadCount == 0) {
                    waitForCompletion(futures);
                    System.out.printf("Ingested [%d] Rejected [%d] in [%d] seconds%n", ingested,
                        rejected, Instant.now().toEpochMilli() - start.toEpochMilli());
                    count = 0;
                }
            }


        } finally {
            System.out.printf("Ingested [%d] Rejected [%d] in [%d] seconds%n", ingested,
                rejected, Instant.now().toEpochMilli() - start.toEpochMilli());
        }


    }

    private Future<WriteRecordsResult> write(WriteRecordsRequest recordsRequest) {
        Future<WriteRecordsResult> future =
            this.executorService.submit(() -> amazonTimestreamWrite.writeRecords(recordsRequest));

        return future;
    }

    private void waitForCompletion(Map<Integer, Future<WriteRecordsResult>> futures) {
        for (Map.Entry<Integer, Future<WriteRecordsResult>> f : futures.entrySet()) {

            final Future<WriteRecordsResult> resultsFuture = f.getValue();

            try {
                resultsFuture.get();
                ingested += 100;
            } catch (Exception e) {
                e.printStackTrace();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                } else if (e instanceof ExecutionException) {
                    final ExecutionException ee = (ExecutionException) e;
                    final Throwable unwrappedException = ee.getCause();
                    if (unwrappedException instanceof RejectedRecordsException) {
                        RejectedRecordsException rre = (RejectedRecordsException) unwrappedException;
                        ingested += 100 - rre.getRejectedRecords().size();
                        rejected += rre.getRejectedRecords().size();

                        for (RejectedRecord r :rre.getRejectedRecords()) {
                            System.out.println(r.getReason());
                        }

                    } else if (unwrappedException instanceof AmazonTimestreamWriteException) {
                        AmazonTimestreamWriteException tswe = (AmazonTimestreamWriteException) unwrappedException;
                        rejected += 100;
                    } else {
                        rejected += 100;
                    }
                } else {
                    rejected += 100;
                }
            }
        }
    }
}
