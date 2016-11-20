package com.epam.training.bigdata.hadoop.highestbid.input.format;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class RecordParser {
    private static final String TAB = "\t";

    public String getUserIdFromLine(String record) {
        int firstIndex = record.indexOf(TAB);
        int secondIndex = record.indexOf(TAB, firstIndex + 1);
        int thirdIndex = record.indexOf(TAB, secondIndex + 1);

        return record.substring(secondIndex + 1, thirdIndex);
    }
}
