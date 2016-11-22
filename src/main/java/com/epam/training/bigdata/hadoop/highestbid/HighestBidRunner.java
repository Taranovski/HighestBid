package com.epam.training.bigdata.hadoop.highestbid;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/16/2016
*/

import com.epam.training.bigdata.hadoop.highestbid.domain.UserAndCounter;
import com.epam.training.bigdata.hadoop.highestbid.input.InputProvider;
import com.epam.training.bigdata.hadoop.highestbid.input.impl.hdfs.HdfsInputProviderImpl;
import com.epam.training.bigdata.hadoop.highestbid.input.impl.local.LocalFileSystemInputProviderImpl;
import com.epam.training.bigdata.hadoop.highestbid.output.OutputProvider;
import com.epam.training.bigdata.hadoop.highestbid.output.impl.hdfs.HdfsOutputProviderImpl;
import com.epam.training.bigdata.hadoop.highestbid.output.impl.local.LocalFileSystemOutputProviderImpl;
import com.epam.training.bigdata.hadoop.highestbid.service.UserCounterService;
import com.epam.training.bigdata.hadoop.highestbid.service.impl.ConcurrentUserCounterServiceImpl;
import com.epam.training.bigdata.hadoop.highestbid.util.Utils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class HighestBidRunner {

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("start time: " + startTime);

        String inputPath = args[0];
        String outputPath = args[1];
        int topNumber = Integer.parseInt(args[2]);

        // local implementation

//        InputProvider inputProvider = new LocalFileSystemInputProviderImpl(inputPath);
//        OutputProvider outputProvider = new LocalFileSystemOutputProviderImpl(outputPath);

        //hdfs implementation

        Configuration configuration = new Configuration();
        //hardcoded hdfs uri here
        configuration.set("fs.default.name", "hdfs://localhost:8020");
        InputProvider inputProvider = new HdfsInputProviderImpl(inputPath, configuration);
        OutputProvider outputProvider = new HdfsOutputProviderImpl(outputPath, configuration);


        UserCounterService userCounterService = new ConcurrentUserCounterServiceImpl();

        Iterator<InputStream> inputStreamIterator = inputProvider.getInputStreamsOfFilesInDirectory();

        Map<String, AtomicLong> userAndCounters = userCounterService.getCalculatedUserCounters(inputStreamIterator);

        System.out.println("map size: " + userAndCounters.size());

        List<UserAndCounter> userAndCounterList = Utils.getSortedUsersAndCountersList(userAndCounters);

        PrintWriter writer = outputProvider.getPrintWriter();

        Utils.writeOutput(writer, userAndCounterList, topNumber);
        long endTime = System.currentTimeMillis();
        System.out.println("end time: " + startTime);
        System.out.println("duration: " + (endTime - startTime));
    }

}
