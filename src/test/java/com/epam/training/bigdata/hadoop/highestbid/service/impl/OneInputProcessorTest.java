package com.epam.training.bigdata.hadoop.highestbid.service.impl;

import com.epam.training.bigdata.hadoop.highestbid.input.format.RecordParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ReturnsArgumentAt;

import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class OneInputProcessorTest {

    RecordParser recordParser = Mockito.mock(RecordParser.class);
    OneInputProcessor oneInputProcessor = new OneInputProcessor(recordParser);

    @Before
    public void before(){
        when(recordParser.getUserIdFromLine(anyString())).thenAnswer(new ReturnsArgumentAt(0));
    }

    @Test
    public void shouldGatherInterMediateResults(){

        Map<String, AtomicLong> stringAtomicLongMap =
                oneInputProcessor.processOneInput(new Scanner("abc\nabd\nabc\nadf"), new ConcurrentHashMap<String, AtomicLong>());

        assertNotNull(stringAtomicLongMap);
        assertFalse(stringAtomicLongMap.isEmpty());
        assertEquals(2L, stringAtomicLongMap.get("abc").get());
        assertEquals(1L, stringAtomicLongMap.get("abd").get());
        assertEquals(1L, stringAtomicLongMap.get("adf").get());

    }

}