package com.epam.training.bigdata.hadoop.highestbid.input.format;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class RecordParserTest {

    RecordParser recordParser = new RecordParser();

    @Test
    public void shouldGetUserIdFromRecord(){
        String record = "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104008\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\tnull\n";

        String userIdFromLine = recordParser.getUserIdFromLine(record);

        assertEquals("Vh16OwT6OQNUXbj", userIdFromLine);
    }
}