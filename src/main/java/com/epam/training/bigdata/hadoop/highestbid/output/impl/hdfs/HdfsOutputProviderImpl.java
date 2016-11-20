package com.epam.training.bigdata.hadoop.highestbid.output.impl.hdfs;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import com.epam.training.bigdata.hadoop.highestbid.output.OutputProvider;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class HdfsOutputProviderImpl implements OutputProvider {
    private final FileSystem fileSystem;
    private final FSDataOutputStream fsDataOutputStream;

    public HdfsOutputProviderImpl(String outputPath, Configuration configuration) {
        try {
            String defaultFileSystem = configuration.get("fs.default.name");
            fileSystem = FileSystem.get(configuration);
            fsDataOutputStream = fileSystem.create(new Path(outputPath));


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PrintWriter getPrintWriter() {
        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, Charsets.UTF_8)));
    }
}
