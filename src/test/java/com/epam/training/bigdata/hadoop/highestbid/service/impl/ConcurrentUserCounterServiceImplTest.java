package com.epam.training.bigdata.hadoop.highestbid.service.impl;

import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.Charsets;
import org.codehaus.plexus.util.StringOutputStream;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class ConcurrentUserCounterServiceImplTest {
    ConcurrentUserCounterServiceImpl concurrentUserCounterService = new ConcurrentUserCounterServiceImpl();

    @Ignore
    @Test
    public void shouldProcessInputAndReturnResult() {
        Iterator<InputStream> inputStreamIterator = new Iterator<InputStream>() {
            List<InputStream> inputStreams = new ArrayList<>();
            {

                StringOutputStream stringOutputStream1 = new StringOutputStream();
                try {
                    CompressorOutputStream bzip2 = new CompressorStreamFactory().createCompressorOutputStream("BZip2", stringOutputStream1);
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abc	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                    bzip2.write("\n".getBytes());
                    bzip2.flush();
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abd	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                    bzip2.write("\n".getBytes());
                    bzip2.flush();
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abc	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                    bzip2.write("\n".getBytes());
                    bzip2.flush();
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abf	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                StringOutputStream stringOutputStream2 = new StringOutputStream();
                try {
                    CompressorOutputStream bzip2 = new CompressorStreamFactory().createCompressorOutputStream("BZip2", stringOutputStream1);
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abd	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                    bzip2.write("\n".getBytes());
                    bzip2.flush();
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abd	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                    bzip2.write("\n".getBytes());
                    bzip2.flush();
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abc	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                    bzip2.write("\n".getBytes());
                    bzip2.flush();
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abc	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                    bzip2.write("\n".getBytes());
                    bzip2.flush();
                    bzip2.write("b382c1c156dcbbd5b9317cb50f6a747b	20130606000104008	abf	mozilla/4.0".getBytes(Charsets.UTF_8));
                    bzip2.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String s1 = stringOutputStream1.toString();
                String s2 = stringOutputStream2.toString();
                inputStreams.add(new ByteArrayInputStream(s1.getBytes(Charsets.UTF_8)));
                inputStreams.add(new ByteArrayInputStream(s2.getBytes(Charsets.UTF_8)));
            }

            Iterator<InputStream> inputStreamIterator = inputStreams.iterator();

            @Override
            public boolean hasNext() {
                return inputStreamIterator.hasNext();
            }

            @Override
            public InputStream next() {
                return inputStreamIterator.next();
            }

            @Override
            public void remove() {
                throw new RuntimeException();
            }
        };
        Map<String, AtomicLong> calculatedUserCounters =
                concurrentUserCounterService.getCalculatedUserCounters(inputStreamIterator);

        assertEquals(3, calculatedUserCounters.size());
        assertEquals(4L, calculatedUserCounters.get("abc").get());
        assertEquals(3L, calculatedUserCounters.get("abd").get());
        assertEquals(2L, calculatedUserCounters.get("abf").get());


    }
}