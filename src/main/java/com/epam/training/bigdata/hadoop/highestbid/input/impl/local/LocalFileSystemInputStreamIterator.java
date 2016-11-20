package com.epam.training.bigdata.hadoop.highestbid.input.impl.local;

import com.epam.training.bigdata.hadoop.highestbid.input.EmptyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class LocalFileSystemInputStreamIterator implements Iterator<InputStream> {
    private final Iterator<Path> locatedFileStatusRemoteIterator;

    public LocalFileSystemInputStreamIterator(Iterator<Path> locatedFileStatusRemoteIterator) {
        this.locatedFileStatusRemoteIterator = locatedFileStatusRemoteIterator;
    }

    @Override
    public boolean hasNext() {
        return locatedFileStatusRemoteIterator.hasNext();
    }

    @Override
    public InputStream next() {
        try {
            Path next = locatedFileStatusRemoteIterator.next();
            System.out.println(next);
            return Files.newInputStream(next);
        } catch (IOException e) {
            e.printStackTrace();
            return EmptyInputStream.get();
        }
    }

    @Override
    public void remove() {
        throw new RuntimeException("remove is not implemented");
    }
}
