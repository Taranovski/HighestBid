package com.epam.training.bigdata.hadoop.highestbid.util;

import com.epam.training.bigdata.hadoop.highestbid.domain.UserAndCounter;
import org.codehaus.plexus.util.StringOutputStream;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class UtilsTest {
    @Test
    public void shouldReturnSortedResult() {
        Map<String, AtomicLong> userAndCounters = new HashMap<>();

        userAndCounters.put("abc", new AtomicLong(2L));
        userAndCounters.put("abd", new AtomicLong(1L));
        userAndCounters.put("abf", new AtomicLong(3L));


        List<UserAndCounter> sortedUsersAndCountersList = Utils.getSortedUsersAndCountersList(userAndCounters);

        assertNotNull(sortedUsersAndCountersList);
        assertEquals(3, sortedUsersAndCountersList.size());

        assertEquals("abf", sortedUsersAndCountersList.get(0).getUserId());
        assertEquals("abc", sortedUsersAndCountersList.get(1).getUserId());
        assertEquals("abd", sortedUsersAndCountersList.get(2).getUserId());

        assertEquals(3, sortedUsersAndCountersList.get(0).getCount());
        assertEquals(2, sortedUsersAndCountersList.get(1).getCount());
        assertEquals(1, sortedUsersAndCountersList.get(2).getCount());

    }

    @Test
    public void shouldWriteTopOutput(){
        StringOutputStream stringOutputStream = new StringOutputStream();

        PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stringOutputStream)));

        List<UserAndCounter> userAndCounterList = new ArrayList<>();

        userAndCounterList.add(UserAndCounter.create("abc", 4));
        userAndCounterList.add(UserAndCounter.create("abf", 3));
        userAndCounterList.add(UserAndCounter.create("abd", 2));
        userAndCounterList.add(UserAndCounter.create("abe", 1));

        Utils.writeOutput(printWriter, userAndCounterList, 3);

        String result = stringOutputStream.toString();

        assertTrue(result.contains("abc\t4"));
        assertTrue(result.contains("abf\t3"));
        assertTrue(result.contains("abd\t2"));
        assertFalse(result.contains("abe\t1"));




    }
}