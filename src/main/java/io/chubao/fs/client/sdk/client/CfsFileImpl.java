package io.chubao.fs.client.sdk.client;

import io.chubao.fs.client.sdk.libsdk.CfsLibrary;
import io.chubao.fs.client.sdk.exception.CfsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CfsFileImpl implements CfsFile {
    private static final Log log = LogFactory.getLog(CfsFileImpl.class);
    private CfsLibrary driver;
    private long clientID;
    private long position = 0L;
    private long fileSize;
    private boolean isClosed = false;
    private int fd;

    public CfsFileImpl(CfsLibrary driver, int fd, long fileSize, long position,long cid) {
        this.driver = driver;
        this.fd = fd;
        this.fileSize = fileSize;
        this.position = position;
        this.clientID=cid;
    }

    public boolean isClosed() {
        return this.isClosed;
    }

    public long getFileSize() {
        return this.fileSize;
    }

    public long getPosition() {
        return this.position;
    }

    public void seek(long position) throws CfsException {


        this.position = position;
    }

    public void close() throws CfsException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        driver.cfs_flush(this.clientID,fd);
        driver.cfs_close(this.clientID,fd);
    }

    @Override
    public void flush() throws CfsException {
        driver.cfs_flush(this.clientID,fd);
    }

    private byte[] buffCopy(byte[] buff, int off, int len) {
        byte[] dest = new byte[len];
        System.arraycopy(buff, off, dest, 0, len);
        return dest;
    }

    public synchronized void write(byte[] buff, int off, int len) throws CfsException {
        if (off < 0 || len < 0) {
            throw new CfsException("Invalid arguments.");
        }

        long wsize = 0;
        if (off == 0) {
            wsize = write(position, buff, len);
        } else {
            byte[] newbuff = buffCopy(buff, off, len);
            wsize = write(position, newbuff, len);
        }

        position += wsize;
        if (position > fileSize) {
            fileSize = position;
        }
    }

    private long write(long offset, byte[] data, int len) throws CfsException {
        return driver.cfs_write(this.clientID,fd, data, len, offset);
    }

    public synchronized long read(byte[] buff, int off, int len) throws CfsException {
        if (off < 0 || len < 0) {
            throw new CfsException("Invalid arguments.");
        }

        long rsize = 0;
        if (off == 0) {
            rsize = driver.cfs_read(this.clientID,fd,  buff, len,position);
        } else {
            byte[] newbuff = new byte[len];
            rsize = driver.cfs_read(this.clientID,fd, newbuff, len, position);
            System.arraycopy(newbuff, 0, buff, off, len);
        }

        if (rsize > 0) {
            position += rsize;
        }
        return rsize;
    }

    @Override
    public void pwrite(byte[] buff, int buffOffset, int len, long fileOffset) throws CfsException {
        throw new CfsException("Not implement.");
    }

    @Override
    public int pread(byte[] buff, int buffOffset, int len, long fileOffset) throws CfsException {
        throw new CfsException("Not implement.");
    }
}

