package com.epam.training.bigdata.hadoop.highestbid.output;

import java.io.PrintWriter;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public interface OutputProvider {
    PrintWriter getPrintWriter();
}
