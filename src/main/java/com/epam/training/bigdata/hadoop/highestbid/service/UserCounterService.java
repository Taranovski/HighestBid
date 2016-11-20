package com.epam.training.bigdata.hadoop.highestbid.service;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public interface UserCounterService {
    Map<String, AtomicLong> getCalculatedUserCounters(Iterator<InputStream> inputStreamIterator);
}
