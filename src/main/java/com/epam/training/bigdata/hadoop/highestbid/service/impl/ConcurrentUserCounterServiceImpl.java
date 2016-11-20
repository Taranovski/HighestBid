package com.epam.training.bigdata.hadoop.highestbid.service.impl;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import com.epam.training.bigdata.hadoop.highestbid.input.decompression.Decompressor;
import com.epam.training.bigdata.hadoop.highestbid.input.format.RecordParser;
import com.epam.training.bigdata.hadoop.highestbid.service.UserCounterService;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentUserCounterServiceImpl implements UserCounterService {

    private static final ComputationEngine COMPUTATION_ENGINE =
            new ComputationEngine(new Decompressor(), new OneInputProcessor(new RecordParser()));

    @Override
    public Map<String, AtomicLong> getCalculatedUserCounters(Iterator<InputStream> inputStreamIterator) {

        ExecutorService executorService = getExecutorService();

        final ConcurrentMap<String, AtomicLong> userAndCounters = new ConcurrentHashMap<>();
        List<Future<Map<String, AtomicLong>>> futures =
                getFutureUserCountingPartialResults(
                        inputStreamIterator, (ConcurrentHashMap<String, AtomicLong>) userAndCounters, executorService);

        collectAllPartialResults(futures);

        executorService.shutdown();

        System.out.println("final size: " + userAndCounters.size());

        return userAndCounters;
    }

    private Map<String, AtomicLong> collectAllPartialResults(List<Future<Map<String, AtomicLong>>> futures) {
        Map<String, AtomicLong> userAndCounters = new ConcurrentHashMap<>();
        for (Future<Map<String, AtomicLong>> mapFuture : futures) {
            Map<String, AtomicLong> stringAtomicLongMap = getCompletedComputationResult(mapFuture);

            System.out.println(stringAtomicLongMap.size());
        }
        return userAndCounters;
    }

    private Map<String, AtomicLong> getCompletedComputationResult(Future<Map<String, AtomicLong>> mapFuture) {
        try {
            return mapFuture.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ExecutorService getExecutorService() {
        return new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    }

    private List<Future<Map<String, AtomicLong>>> getFutureUserCountingPartialResults(
            Iterator<InputStream> inputStreamIterator,
            ConcurrentHashMap<String, AtomicLong> intermediateResults,
            ExecutorService executorService) {
        List<Future<Map<String, AtomicLong>>> futures = new ArrayList<>();

        while (inputStreamIterator.hasNext()) {
            InputStream open = inputStreamIterator.next();

            Future<Map<String, AtomicLong>> submit =
                    executorService.submit(COMPUTATION_ENGINE.getTaskForFileProcessing(open, intermediateResults));
            futures.add(submit);
        }
        return futures;
    }

}
