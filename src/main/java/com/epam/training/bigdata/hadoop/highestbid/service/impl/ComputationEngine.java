package com.epam.training.bigdata.hadoop.highestbid.service.impl;

import com.epam.training.bigdata.hadoop.highestbid.input.decompression.Decompressor;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class ComputationEngine {


    private final Decompressor decompressor;
    private final OneInputProcessor oneInputProcessor;

    public ComputationEngine(Decompressor decompressor, OneInputProcessor oneInputProcessor) {
        this.decompressor = decompressor;
        this.oneInputProcessor = oneInputProcessor;
    }

    public Callable<Map<String, AtomicLong>> getTaskForFileProcessing(
            final InputStream open,
            final ConcurrentMap<String, AtomicLong> intermediateResults) {
        return new Callable<Map<String, AtomicLong>>() {
            @Override
            public Map<String, AtomicLong> call() throws Exception {
                InputStream compressorInputStream = getBufferedAndDecompressedInputStream(open);
                Scanner scanner = new Scanner(compressorInputStream);
                long initialTime = System.currentTimeMillis();
                Map<String, AtomicLong> stringAtomicLongMap =
                        oneInputProcessor.processOneInput(scanner, intermediateResults);
                long endTime = System.currentTimeMillis();
                System.out.println("processed in: " + (endTime - initialTime));
                return stringAtomicLongMap;
            }
        };
    }

    private InputStream getBufferedAndDecompressedInputStream(InputStream open) {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(open);
        return decompressor.getDecompressedInputStream(bufferedInputStream);
    }

}
