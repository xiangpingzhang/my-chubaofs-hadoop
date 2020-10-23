package io.chubao.fs.client.stream;

import com.google.common.base.Preconditions;
import io.chubao.fs.client.sdk.client.CfsFile;
import io.chubao.fs.client.sdk.exception.CfsEOFException;
import io.chubao.fs.client.sdk.exception.CfsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

@Public
@Evolving
public class CfsDataInputStream extends InputStream  implements ByteBufferReadable,HasEnhancedByteBufferAccess,CanUnbuffer,Seekable, PositionedReadable{
    private static final Log log=LogFactory.getLog(CfsDataInputStream.class);
    private CfsFile cFile;

    public CfsDataInputStream(CfsFile cFile){
        this.cFile=cFile;
    }

    @Override
    public int available() throws IOException{
        return (int)(cFile.getFileSize()-cFile.getPosition());
    }

    @Override
    public int read() throws IOException {
        byte buff[] = new byte[1];
        int bread = read(buff, 0, 1);
        if (bread <= 0) { // no content read
            return bread;
        }

        return (buff[0] & 0xFF);
    }


    public void seek(long pos) throws IOException {
        if (pos > cFile.getFileSize()) {
            throw new EOFException("The pos: " + pos + " is more than file size: " + cFile.getFileSize());
        }

        try {
            cFile.seek(pos);
        } catch (CfsException ex) {
            throw new IOException();
        }
    }




    //get position in the file
    public long getPos() throws IOException{
        return cFile.getPosition();
    }

    //select a copy again in multiple copies of file data
    public boolean seekToNewSource(long targetPos) throws IOException {
        return targetPos > cFile.getFileSize() ? false : true;
    }

    @Override
    public synchronized int read(final byte buf[], int off, int len) throws IOException {
        if (buf == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        long size =0;
        try {
            size = (int)cFile.read(buf, off, len);
            return (int) size;
        } catch (CfsEOFException e) {
           return -1;
        } catch (CfsException ex) {
            throw new IOException();
        }
    }


    public int read(ByteBuffer byteBuffer) throws IOException {
        byte[] data = new byte[byteBuffer.remaining()];
        int rSize = read(data);

        if (rSize > 0) {
            byteBuffer.put(data);
        }
        return rSize;
    }


    @Override
    public ByteBuffer read(ByteBufferPool byteBufferPool, int i, EnumSet<ReadOption> enumSet) throws IOException, UnsupportedOperationException {
        throw new IOException("Not implement the read function.");
    }

    @Override
    public void unbuffer() {
        log.error("Not implement unbuffer function.");
    }

    @Override
    public void releaseBuffer(ByteBuffer byteBuffer) {
        log.error("Not implement releaseBuff function.");
    }

    @Override
    public void close() throws IOException {
        try {
            cFile.close();
        } catch (CfsException ex) {
            throw new IOException();
        }
    }

    @Override
    public int read(long l, byte[] bytes, int i, int i1) throws IOException {
        return 0;
    }

    @Override

    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        this.validatePositionedReadArgs(position, buffer, offset, length);

        int nbytes;
        for(int nread = 0; nread < length; nread += nbytes) {
            nbytes = this.read(position + (long)nread, buffer, offset + nread, length - nread);
            if (nbytes < 0) {
                throw new EOFException("End of file reached before reading fully.");
            }
        }

    }
    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        this.readFully(position, buffer, 0, buffer.length);
    }
    protected void validatePositionedReadArgs(long position, byte[] buffer, int offset, int length) throws EOFException {
        Preconditions.checkArgument(length >= 0, "length is negative");
        if (position < 0L) {
            throw new EOFException("position is negative");
        } else {
            Preconditions.checkArgument(buffer != null, "Null buffer");
            if (buffer.length - offset < length) {
                throw new IndexOutOfBoundsException("Requested more bytes than destination buffer size: request length=" + length + ", with offset =" + offset + "; buffer capacity =" + (buffer.length - offset));
            }
        }
    }
}

