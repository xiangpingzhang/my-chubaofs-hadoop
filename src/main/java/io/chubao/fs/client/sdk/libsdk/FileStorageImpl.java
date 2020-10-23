// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package io.chubao.fs.client.sdk.libsdk;

import com.sun.jna.Pointer;
import io.chubao.fs.client.sdk.client.CfsFile;
import io.chubao.fs.client.sdk.client.CfsFileImpl;
import io.chubao.fs.client.sdk.client.CfsLibrary;
import io.chubao.fs.client.sdk.exception.*;
import io.chubao.fs.client.util.CfsOwnerHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStorageImpl implements FileStorage {
    private static final Log log = LogFactory.getLog(FileStorageImpl.class);
    private CfsLibrary cfsLib;
    private long clientID;
    private CfsOwnerHelper owner;
    private long defaultBlockSize = 128 * 1024 * 1024;
    private int defaultDirPermission = 0644;

    private final static int ATTR_MODE = 1 << 0;
    private final static int ATTR_UID = 1 << 1;
    private final static int ATTR_GID = 1 << 2;
    private final static int ATTR_MTIME = 1 << 3;
    private final static int ATTR_ATIME = 1 << 4;
    private final static int ATTR_SIZE = 1 << 5;
    private final static int batchSize = 100;
    private final static int timeFactor = 1000 * 1000 * 1000;

    public FileStorageImpl(CfsLibrary cfsLib, long cid) {
        this.cfsLib = cfsLib;
        this.clientID = cid;
    }

    public void init() throws Exception {
        owner = new CfsOwnerHelper();
        owner.init();
    }


    @Override
    public CfsFile open(String path, int flags, int mode, int uid, int gid) throws CfsException {
        int flagsTmp = flags;
        if ((flags & FileStorage.O_APPEND) != 0) {
            flags = flags & ~(FileStorage.O_APPEND);
        }
        int fd = open1(path, flags, mode, uid, gid);
        long size = cfsLib.cfs_file_size(this.clientID, fd);
        long pos = 0L;
        if ((flagsTmp & FileStorage.O_APPEND) != 0) {
            pos = size;
        }
        if (log.isDebugEnabled()) {
            log.debug("Success to open:" + path + " size:" + size + " pos:" + pos);
        }
        return new CfsFileImpl(cfsLib, fd, size, pos, this.clientID);
    }

    @Override
    public boolean mkdirs(String path, int mode, int uid, int gid) throws CfsException {
        verifyPath(path);
        int st = cfsLib.cfs_mkdirs(this.clientID, path, mode, uid, gid);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK &&
                StatusCodes.get(st) != StatusCodes.CFS_STATUS_FILE_EXISTS) {
            throw new CfsException("Failed to mkdirs: " + path + " status code:" + st);
        }
        return true;
    }

    @Override
    public void truncate(String path, long newLength) throws CfsException {
        if (newLength < 0) {
            throw new CfsException("Invalid arguments.");
        }
        verifyPath(path);

        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        stat.size = newLength;
        int valid = ATTR_SIZE;
        int st = cfsLib.cfs_setattr_by_path(this.clientID, path, stat, valid);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to truncate: " + path + " status code: " + st);
        }
    }

    @Override
    public void close () throws CfsException {
        cfsLib.cfs_close_client(this.clientID);
    }

    public void close(int fd) throws CfsException {
        if (fd < 1) {
            throw new CfsException("Invalid arguments.");
        }
        cfsLib.cfs_close(this.clientID, fd);
    }

    @Override
    public void rmdir (String path,boolean recursive) throws CfsException {
        verifyPath(path);
        int st = cfsLib.cfs_rmdir(this.clientID, path, recursive);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to rmdir:" + path + " status code:" + st);
        }
    }

    @Override
    public void unlink (String path) throws CfsException {
        verifyPath(path);
        int st = cfsLib.cfs_unlink(this.clientID, path);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to unlink " + path + ", the status code is " + st);
        }
    }

    @Override
    public void rename (String from, String to) throws CfsException {
        verifyPath(from);
        verifyPath(to);
        int st = cfsLib.cfs_rename(this.clientID, from, to);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to rename: " + from + " to:" + to + " status code:" + st);
        }
    }

    @Override
    public CfsStatInfo[] list (String path) throws CfsException {
        int fd = 0;
        ArrayList<CfsStatInfo> stats = new ArrayList<CfsStatInfo>();
        try {
            fd = open1(path, O_RDONLY, defaultDirPermission, 0, 0);
            long total = 0;
            long num = 0;
            while (true) {
                num = list(fd, stats);
                if (num <= 0) {
                    break;
                }
                total += num;
            }

        } catch (CfsException ex) {
            throw ex;
        } catch (UnsupportedEncodingException e) {
            throw new CfsException(e);
        } finally {
            if (fd > 0) {
                close(fd);
            }
        }
        CfsStatInfo[] res = new CfsStatInfo[stats.size()];
        stats.toArray(res);
        return res;
    }

    @Override
    public CfsStatInfo stat (String path) throws CfsException {
        CfsLibrary.StatInfo info = getAttr(path);
        if (info == null) {
            return null;
        }
        return new CfsStatInfo(
                info.mode, info.uid, info.gid, info.size,
                info.ctime, info.mtime, info.atime);
    }

    @Override
    public void setXAttr (String path, String name,byte[] value) throws CfsException {
        throw new CfsException("Not implement setXAttr.");
    }

    @Override
    public byte[] getXAttr (String path, String name) throws CfsException {
        throw new CfsException("Not implement getXAttr.");
    }

    @Override
    public List<String> listXAttr (String path) throws CfsException {
        throw new CfsException("Not implement listXAttr.");
    }

    @Override
    public Map<String, byte[]> getXAttrs (String path, List < String > names)
            throws CfsException {
        throw new CfsException("Not implement getXAttrs.");
    }

    @Override
    public void removeXAttr (String path, String name) throws CfsException {
        log.error("Not implement.");
        throw new CfsException("Not implement removeXAttr.");
    }

    @Override
    public void chmod (String path,int mode) throws CfsException {
        verifyPath(path);
        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        stat.mode = mode;
        int valid = ATTR_MODE;
        int st = cfsLib.cfs_setattr_by_path(this.clientID, path, stat, valid);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to chmod: " + path + " status code: " + st);
        }
    }

    @Override
    public void chown (String path,int uid, int gid) throws CfsException {
        System.out.println("path:"+path);
        System.out.println("uid:"+uid);
        System.out.println("gid:"+gid);
        verifyPath(path);
        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        stat.uid = uid;
        stat.gid = gid;
        int valid = ATTR_GID | ATTR_UID;
        int st = cfsLib.cfs_setattr_by_path(this.clientID, path, stat, valid);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to chown: " + path + " status code: " + st);
        }
    }

    @Override
    public void chown (String path, String user, String group) throws CfsException {
        chown(path, owner.getUid(user), owner.getGid(group));
    }

    @Override
    public void setTimes (String path,long mtime, long atime) throws CfsException {
        verifyPath(path);
        if (log.isDebugEnabled()) {
            log.debug("settimes:" + path + " mtime:" + mtime + " atime:" + atime);
        }
        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        int valid = 0;
        if (mtime > 0) {
            stat.mtime = mtime / timeFactor;
            stat.mtime_nsec = (int) (mtime % timeFactor);
            valid = ATTR_MTIME;
        }

        if (atime > 0) {
            stat.atime = atime / timeFactor;
            stat.atime_nsec = (int) (atime % timeFactor);
            valid = valid | ATTR_ATIME;
        }
        int st = cfsLib.cfs_setattr_by_path(this.clientID, path, stat, valid);
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to settimes: " + path + " status code: " + st);
        }
    }

    @Override
    public int getUid (String username) throws CfsException {
        return owner.getUid(username);
    }

    @Override
    public int getGid (String group) throws CfsException {
        return owner.getGid(group);
    }

    @Override
    public int getGidByUser (String user) throws CfsException {
        return owner.getGidByUser(user);
    }

    @Override
    public String getUser ( int uid) throws CfsException {
        return owner.getUser(uid);
    }

    @Override
    public String getGroup ( int gid) throws CfsException {
        return owner.getGroup(gid);
    }

    private void setAttr (String path,int mode, int uid, int gid, long mtime, long atime)
            throws CfsException {
        throw new CfsException("Not implement setAttr.");
    }

    @Override
    public long getBlockSize () {
        return defaultBlockSize;
    }


    @Override
    public int getReplicaNumber () {
        return 3;
    }


    public long size(int fd) throws CfsException {
        long size = cfsLib.cfs_file_size(this.clientID, fd);
        if (size < 0) {
            throw new CfsException("Failed to get size of file:" + fd + " status code: " + size);
        }
        return size;
    }

    //this open return a int
    public int open1(String path, int flags, int mode, int uid, int gid) throws CfsException {
        verifyPath(path);

        int st = cfsLib.cfs_open(this.clientID, path, flags, mode, uid, gid);
        if (st < 0) {
            throw new CfsException("Failed to open:" + path + " status code: " + st);
        }

        return st;
    }

    //get attr
    public CfsLibrary.StatInfo getAttr(String path) throws CfsException {
        verifyPath(path);
        CfsLibrary.StatInfo.ByReference info = new CfsLibrary.StatInfo.ByReference();
        int st = cfsLib.cfs_getattr(this.clientID, path, info);
        if (StatusCodes.get(st) == StatusCodes.CFS_STATUS_FILIE_NOT_FOUND) {
            log.info("Not found the path: " + path + " error code: " + st);
            return null;
        }
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            log.error("Not stat the path: " + path + " error code: " + st);
            throw new CfsException("Failed to stat.");
        }
        return info;
    }

    //verify path
    private void verifyPath(String path) throws CfsException {
        if (path == null || path.trim().length() == 0) {
            throw new CfsNullArgumentException("path is invlaid.");
        }

        if (path.startsWith("/") == false) {
            throw new CfsInvalidArgumentException(path);
        }
    }

    public int list(int fd, ArrayList<CfsStatInfo> fileStats) throws CfsException, UnsupportedEncodingException {
        CfsLibrary.Dirent dent = new CfsLibrary.Dirent();
        CfsLibrary.Dirent[] dents = (CfsLibrary.Dirent[]) dent.toArray(batchSize);

        Pointer arr = dents[0].getPointer();
        CfsLibrary.DirentArray.ByValue slice = new CfsLibrary.DirentArray.ByValue();
        slice.data = arr;
        slice.len = batchSize;
        slice.cap = batchSize;

        int count = cfsLib.cfs_readdir(this.clientID, fd, slice, batchSize);
        if (StatusCodes.get(count) == StatusCodes.CFS_STATUS_FILIE_NOT_FOUND) {
            throw new CfsFileNotFoundException("Not found " + fd);
        }
        if (count < 0) {
            throw new CfsException("Failed to count dir:" + fd + " status code: " + count);
        }

        if (count == 0) {
            return count;
        }

        long[] iids = new long[count];
        Map<Long, String> names = new HashMap<Long, String>(count);
        for (int i = 0; i < count; i++) {
            dents[i].read();
            iids[i] = dents[i].ino;
            names.put(dents[i].ino, new String(dents[i].name, 0, dents[i].nameLen, "utf-8"));
        }

        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        CfsLibrary.StatInfo[] stats = (CfsLibrary.StatInfo[]) stat.toArray(count);
        Pointer statsPtr = stats[0].getPointer();
        CfsLibrary.DirentArray.ByValue statSlice = new CfsLibrary.DirentArray.ByValue();
        statSlice.data = statsPtr;
        statSlice.len = batchSize;
        statSlice.cap = batchSize;
        int num = cfsLib.cfs_batch_get_inodes(this.clientID, fd, iids, statSlice, count);
        if (num < 0) {
            throw new CfsException("Failed to get inodes,  the fd:" + fd + " status code: " + num);
        }

        for (int i = 0; i < num; i++) {
            stats[i].read();
            CfsLibrary.StatInfo in = stats[i];
            try {
                CfsStatInfo info = new CfsStatInfo(
                        in.mode, in.uid, in.gid, in.size,
                        in.ctime, in.mtime, in.atime, names.get(in.ino));
                fileStats.add(info);

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return num;
    }
}