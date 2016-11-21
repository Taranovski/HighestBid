package com.epam.training.bigdata.hadoop.highestbid.service.impl;

import com.epam.training.bigdata.hadoop.highestbid.input.format.RecordParser;

import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class OneInputProcessor {
    private final RecordParser recordParser;

    public OneInputProcessor(RecordParser recordParser) {
        this.recordParser = recordParser;
    }

    public Map<String, AtomicLong> processOneInput(Scanner scanner, ConcurrentMap<String, AtomicLong> intermediateResults) {
        while (scanner.hasNextLine()) {
            String nextLine = scanner.nextLine();
            String userId = recordParser.getUserIdFromLine(nextLine);

            intermediateResults.putIfAbsent(userId, new AtomicLong(0L));
            intermediateResults.get(userId).incrementAndGet();
        }
        scanner.close();
        return intermediateResults;
    }

}
