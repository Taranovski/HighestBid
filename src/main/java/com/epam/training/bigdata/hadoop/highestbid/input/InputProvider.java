package com.epam.training.bigdata.hadoop.highestbid.input;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import java.io.InputStream;
import java.util.Iterator;

public interface InputProvider {
    Iterator<InputStream> getInputStreamsOfFilesInDirectory();
}
