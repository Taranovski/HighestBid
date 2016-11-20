package com.epam.training.bigdata.hadoop.highestbid.input.impl.hdfs;

import com.epam.training.bigdata.hadoop.highestbid.input.InputProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class HdfsInputProviderImpl implements InputProvider {

    private final RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator;
    private final FileSystem fileSystem;

    public HdfsInputProviderImpl(String inputPath, Configuration conf) {
        try {
            fileSystem = FileSystem.get(conf);
            locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path(inputPath), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<InputStream> getInputStreamsOfFilesInDirectory() {
        return new HdfsInputStreamIterator(fileSystem, locatedFileStatusRemoteIterator);
    }
}
