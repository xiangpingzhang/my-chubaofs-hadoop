package io.chubao.fs.client.sdk.client;

import io.chubao.fs.client.sdk.exception.CfsException;

import java.io.IOException;

public interface CfsFile {
    void close() throws CfsException;

    void flush() throws  CfsException;

    void write(byte[] buff, int buffOffset, int len) throws CfsException;

    //File atomic write
    void pwrite(byte[] buff, int buffOffset, int len, long fileOffset) throws CfsException;

    void seek(long offset) throws CfsException;

    long read(byte[] buff, int buffOffset, int len) throws CfsException ;

    //File atomic read
    int pread(byte[] buff, int buffOffset, int len, long fileOffset) throws CfsException;

    long getFileSize();

    long getPosition();
}
