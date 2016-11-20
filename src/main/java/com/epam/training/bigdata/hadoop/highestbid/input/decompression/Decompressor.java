package com.epam.training.bigdata.hadoop.highestbid.input.decompression;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.InputStream;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/
public class Decompressor {

    private static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY = new CompressorStreamFactory();

    public InputStream getDecompressedInputStream(InputStream inputStream) {
        try {
            return COMPRESSOR_STREAM_FACTORY.createCompressorInputStream(inputStream);
        } catch (CompressorException e) {
            throw new RuntimeException(e);
        }
    }

}
