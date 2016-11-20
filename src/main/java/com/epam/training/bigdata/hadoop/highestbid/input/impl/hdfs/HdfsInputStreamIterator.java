package com.epam.training.bigdata.hadoop.highestbid.input.impl.hdfs;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class HdfsInputStreamIterator implements Iterator<InputStream> {
    private final FileSystem fileSystem;
    private final RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator;

    public HdfsInputStreamIterator(FileSystem fileSystem,
                                   RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator) {
        this.fileSystem = fileSystem;
        this.locatedFileStatusRemoteIterator = locatedFileStatusRemoteIterator;
    }

    @Override
    public boolean hasNext() {
        try {
            return locatedFileStatusRemoteIterator.hasNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream next() {
        try {
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            Path path = locatedFileStatus.getPath();
            return fileSystem.open(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new RuntimeException("remove is not implemented");
    }
}
