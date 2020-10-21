package io.chubao.fs.client.stream;

import io.chubao.fs.client.sdk.client.CfsFile;

import java.io.IOException;
import java.io.OutputStream;

public class CfsDataOutputStream extends OutputStream {
    private CfsFile cFile;

    public CfsDataOutputStream(CfsFile file){
        this.cFile=file;
    }
    @Override
    public void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }
}
