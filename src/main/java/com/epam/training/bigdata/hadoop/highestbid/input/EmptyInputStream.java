package com.epam.training.bigdata.hadoop.highestbid.input;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import java.io.IOException;
import java.io.InputStream;

public class EmptyInputStream extends InputStream {

    public static InputStream get() {
        return new EmptyInputStream();
    }

    private EmptyInputStream() {
    }

    @Override
    public int read() throws IOException {
        return -1;
    }
}
