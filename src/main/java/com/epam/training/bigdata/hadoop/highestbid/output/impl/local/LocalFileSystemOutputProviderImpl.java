package com.epam.training.bigdata.hadoop.highestbid.output.impl.local;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import com.epam.training.bigdata.hadoop.highestbid.output.OutputProvider;
import com.google.common.base.Charsets;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalFileSystemOutputProviderImpl implements OutputProvider {

    private PrintWriter printWriter;

    public LocalFileSystemOutputProviderImpl(String outputPath) {
        try {
            Path path = Paths.get(outputPath);
            Files.createDirectories(path.getParent());
            Files.createFile(path);
            printWriter = new PrintWriter(new BufferedWriter(Files.newBufferedWriter(path, Charsets.UTF_8)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PrintWriter getPrintWriter() {
        return printWriter;
    }
}
