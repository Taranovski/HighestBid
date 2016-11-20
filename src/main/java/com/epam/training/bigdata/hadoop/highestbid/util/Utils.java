package com.epam.training.bigdata.hadoop.highestbid.util;

import com.epam.training.bigdata.hadoop.highestbid.domain.UserAndCounter;
import com.epam.training.bigdata.hadoop.highestbid.domain.UserAndCounterComparator;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class Utils {

    public static void writeOutput(PrintWriter writer, List<UserAndCounter> userAndCounterList, int topNumber) {
        int max = Math.min(topNumber, userAndCounterList.size());

        int flushTreshold = 10000;

        for (int i = 0; i < max; i++) {
            UserAndCounter userAndCounter = userAndCounterList.get(i);
            writer.println(userAndCounter.getUserId() + "\t" +  userAndCounter.getCount());
            if (i % flushTreshold == 0) {
                writer.flush();
            }
        }
        writer.flush();
    }

    public static List<UserAndCounter> getSortedUsersAndCountersList(Map<String, AtomicLong> userAndCounters) {
        List<UserAndCounter> userAndCounterList = new ArrayList<>(userAndCounters.size());

        for (Map.Entry<String, AtomicLong> stringAtomicLongEntry : userAndCounters.entrySet()) {
            userAndCounterList.add(
                    UserAndCounter.create(stringAtomicLongEntry.getKey(), stringAtomicLongEntry.getValue().get())
            );
        }

        Collections.sort(userAndCounterList, new UserAndCounterComparator());

        return userAndCounterList;
    }

}
