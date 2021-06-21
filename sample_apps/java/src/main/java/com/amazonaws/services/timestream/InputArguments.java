package com.amazonaws.services.timestream;

import org.kohsuke.args4j.Option;

public class InputArguments {

    @Option(name = "--inputFile", aliases = "-i", usage = "input to the csv file path for ingestion")
    public String inputFile;

    @Option(name = "--kmsId", aliases = "-k", usage = "kmsId for update")
    public String kmsId;

    @Option(name = "--threadCount", aliases = "-t", usage = "thread count for concurrency")
    public int threadCount;

}
