package com.epam.training.bigdata.hadoop.highestbid.input.impl.local;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import com.epam.training.bigdata.hadoop.highestbid.input.InputProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Iterator;

public class LocalFileSystemInputProviderImpl implements InputProvider {

    private final DirectoryStream<Path> paths;

    public LocalFileSystemInputProviderImpl(String inputPath) {
        FileSystem fileSystem = FileSystems.getDefault();
        Path path1 = fileSystem.getPath(inputPath);
        try {
            paths = Files.newDirectoryStream(path1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<InputStream> getInputStreamsOfFilesInDirectory() {
        Iterator<Path> locatedFileStatusRemoteIterator = paths.iterator();

        return new LocalFileSystemInputStreamIterator(locatedFileStatusRemoteIterator);
    }

}
