package io.chubao.fs.client.stream;

import io.chubao.fs.client.sdk.client.CfsFile;
import io.chubao.fs.client.sdk.exception.CfsException;

import java.io.IOException;
import java.io.OutputStream;

public class CfsDataOutputStream extends OutputStream {
    private CfsFile cFile;

    public CfsDataOutputStream(CfsFile file){
        this.cFile=file;
    }

    @Override
    public void close() throws IOException {
        try {
            cFile.close();
            super.close();
        } catch (CfsException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            cFile.flush();
        } catch (CfsException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            cFile.write(b, off, len);
        } catch (CfsException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }
}
