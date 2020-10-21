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

package org.apache.hadoop.hdfs;

import io.chubao.fs.client.config.CfsConfig;
import io.chubao.fs.client.sdk.client.*;
import io.chubao.fs.client.sdk.exception.*;
import io.chubao.fs.client.sdk.libsdk.*;
import io.chubao.fs.client.stream.*;
import io.chubao.fs.client.util.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;

@InterfaceAudience.LimitedPrivate({"MapReduce","Hbase"})
@InterfaceStability.Unstable
public class ChubaoFileSystem extends FileSystem {
    private static final Log log = LogFactory.getLog(ChubaoFileSystem.class);
    private final String CFS_SCHEME_NAME = "cfs";
    private final String CFS_SITE_CONFIG = "cfs-site.xml";

    private URI uri;
    private CfsConfig cfg;
    private CfsMount client;
    private FileStorage storage;
    private int uid;
    private int gid;
    private Path workingDir;
    private String userHomePrefix;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        System.out.println("my chubaofs hadoop");
        log.info("==> Initialize ChubaoFileSystem.");
        //get and check the Uri
        if (!uri.getScheme().equalsIgnoreCase(CFS_SCHEME_NAME)) {
            throw new IllegalArgumentException("Note support the scheme:" + uri.toString() + ",you may be use [cfs://]");
        }
        //initialize  uri
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        super.initialize(uri, conf);

        //get ConfDir from operation system or java system
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir == null) {
            hadoopConfDir = System.getProperty("HADOOP_CONF_DIR");
        }
        if (hadoopConfDir == null) {
            throw new IllegalArgumentException("HADOOP_CONF_DIR env or property has not been set!");
        }
        //initial  configFile dir
        String configFile = hadoopConfDir + File.separator + CFS_SITE_CONFIG;
        String userName = System.getProperty("user.name");
        try {
            cfg = new CfsConfig();
            //load and then parse config
            cfg.load(configFile);
            //get CfsMount
            client = new CfsMount(cfg.getCfsLibsdk());
            //get config and set client
            StorageConfig sConf = getStorageConfig(cfg);
            storage = client.openFileStorage(sConf,client);
            uid = storage.getUid(userName);
            gid = storage.getGidByUser(userName);
            userHomePrefix = cfg.getUserHomePrefix();
            workingDir = getHomeDirectory();
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            cfg.setCurrentUser(currentUser.getUserName());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IOException("Failed to initialize ChubaoFileSystem", e);
        }
    }

    private StorageConfig getStorageConfig(CfsConfig cfg) {
        StorageConfig config = new StorageConfig();
        config.setMasters(cfg.getCfsMasterAddr());
        config.setVolumeName(cfg.getCfsVolumeName());
        config.setOwner(cfg.getCfsVolumeOwner());
        String logDir = cfg.getCfsLogDir();
        if (logDir != null) {
            config.setLogDir(logDir);
        }
        String logLevel = cfg.getCfsLogLevel();
        if (logLevel != null) {
            config.setLogLevel(logLevel);
        }

        config.print();
        return config;
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void close() throws IOException {
        log.info("Close ChubaoFileSystem");
        super.close();
    }

    @Override
    public String getScheme() {
        return CFS_SCHEME_NAME;
    }

    @Override
    public Path getHomeDirectory() {
        return makeQualified(new Path(userHomePrefix + "/" + System.getProperty("user.name")));
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return mkdirs(f, FsPermissionHelper.getDirDefault(cfg));
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("open:" + path.toString() + " buffsize:" + i);
        }
        statistics.incrementReadOps(1);
        try {
            CfsFile cFile = storage.open(parsePath(path), FileStorage.O_RDONLY, 0, uid, gid);
            return new FSDataInputStream(new CfsDataInputStream(cFile));
        } catch (Exception ex) {
            log.error("Failed to open:" + path.toString());
            throw new IOException(ex);
        }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Create path: " + path.toString() + " permission: "
                    + Integer.toHexString((int) permission.toShort()) + " overwrite: " + overwrite + " bufferSize: " + bufferSize
                    + " replication: " + replication + " blockSize: " + blockSize + " progress: " + progress);
        }
        statistics.incrementWriteOps(1);
        CfsFile cfile = null;
        try {
            Path parentPath = path.getParent();
            boolean res = mkdirs(parentPath, permission);
            if (!res) {
                throw new IOException("Failed to mkdirs:" + parentPath.toString());
            }

            String pathStr = parsePath(path);
            if (exists(path)) {
                if (overwrite) {
                    cfile = storage.open(pathStr, FileStorage.O_WRONLY | FileStorage.O_TRUNC, permission.toShort(), uid, gid);
                } else {
                    CfsStatInfo stat = storage.stat(pathStr);
                    if (stat != null) {
                        throw new FileAlreadyExistsException(pathStr);
                    }
                    cfile = storage.open(pathStr, FileStorage.O_WRONLY | FileStorage.O_CREAT, permission.toShort(), uid, gid);
                }
            } else {
                cfile = storage.open(pathStr, FileStorage.O_WRONLY | FileStorage.O_CREAT, permission.toShort(), uid, gid);
            }

            if (progress != null) {
                progress.progress();
            }

            CfsDataOutputStream output = new CfsDataOutputStream(cfile);
            return new FSDataOutputStream(output, statistics);
        } catch (CfsFileNotFoundException e) {
            throw new FileNotFoundException(e.getMessage());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new IOException(ex);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        FsPermission umask = FsPermission.getUMask(getConf());
        FsPermission perm = FsPermissionHelper.getFileDefault(cfg).applyUMask(umask);
        return this.create(f, perm, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progress) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("append:" + path.toString() + " buffsize:" + i);
        }
        statistics.incrementWriteOps(1);
        try {
            if (progress != null) {
                progress.progress();
            }
            int flags = FileStorage.O_WRONLY | FileStorage.O_APPEND;
            CfsFile cFile = storage.open(parsePath(path), flags, cfg.CFS_DEFAULT_FILE_PERMISSION, uid, gid);
            CfsDataOutputStream output = new CfsDataOutputStream(cFile);
            return new FSDataOutputStream(output, statistics);
        } catch (Exception ex) {
            log.error("Failed to append:" + path.toString());
            throw new IOException(ex);
        }
    }

    @Override
    public short getDefaultReplication() {
        return (short) storage.getReplicaNumber();
    }

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("truncate:" + f.toString() + " newlength:" + newLength);
        }
        statistics.incrementWriteOps(1);

        FileStatus status = getFileStatus(f);
        if (status == null || status.isDirectory() || status.getLen() <= newLength) {
            return false;
        }

        try {
            storage.truncate(parsePath(f), newLength);
        } catch (Exception ex) {
            log.error("Failed to truncate:" + f.toString());
            throw new IOException(ex);
        }
        return true;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("rename:" + src.toString() + " to:" + dst.toString());
        }
        statistics.incrementWriteOps(1);
        String from = null;
        String to = null;
    /*
    If src and dst are file, and are exists.
    1. src and dst are the  same file, return true.
       the case in testMoveFileUnderParent
    2. else not, return false.
       the case in testRenameFileAsExistingFile
     */
        if (LOG.isDebugEnabled()) {
            LOG.debug("RenameEntry:" + src + " to:" + dst);
        }
        try {
            from = parsePath(src);
            to = parsePath(dst);
            CfsStatInfo srcInfo = storage.stat(from);
            if (srcInfo == null) {
                return false;
            }
            if (srcInfo.getType() == CfsStatInfo.Type.REG) {
                CfsStatInfo dstInfo = storage.stat(to);
                if (dstInfo != null && dstInfo.getType() == CfsStatInfo.Type.REG) {
                    if (src == dst) {
                        return true;
                    }
                    return false;
                }
            }
        } catch (CfsException ex) {
            log.error("Failed to rename:" + src + " to:" + dst);
            throw new IOException(ex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Rename:" + from + " to:" + to);
        }
        try {
            storage.rename(from, to);
            return true;
        } catch (CfsException ex) {
            log.error("Failed to rename:" + src + " to:" + dst);
            log.error(ex.getMessage(), ex);
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("delete:" + path.toString() + " recursive:" + recursive);
        }
        statistics.incrementWriteOps(1);

        String str = null;
        try {
            str = parsePath(path);
            CfsStatInfo info = storage.stat(str);
            if (info == null) {
                return false;
                //throw new FileNotFoundException(path.toString());
            }

            if (info.getType() == CfsStatInfo.Type.DIR) {
                storage.rmdir(str, recursive);
            } else if (info.getType() == CfsStatInfo.Type.REG || info.getType() == CfsStatInfo.Type.LINK) {
                storage.unlink(str);
            } else {
                throw new IOException("Not support the type:" + info.getType());
            }
        } catch (Exception ex) {
            log.error("Failed to delete:" + path.toString());
            throw new IOException(ex);
        }
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        if (log.isDebugEnabled()) {
            log.debug("list:" + path.toString());
        }
        try {
            FileStatus[] fStatus = null;
            FileStatus status = getFileStatus(path);
            if (status != null && status.isDirectory() == false) {
                fStatus = new FileStatus[1];
                fStatus[0] = status;
                return fStatus;
            }

            String pathStr = parsePath(path);
            CfsStatInfo[] infos = storage.list(pathStr);
            fStatus = new FileStatus[infos.length];
            for (int i = 0; i < infos.length; i++) {
                fStatus[i] = FileStatusHelper.convert(storage, (uri == null ? null : uri.toString()), pathStr, infos[i]);
                log.info("==>" + fStatus[i]);
            }
            return fStatus;
        } catch (CfsFileNotFoundException e) {
            throw new FileNotFoundException(e.getMessage());
        } catch (CfsException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void setWorkingDirectory(Path path) {
        this.workingDir = fixRelativePart(path);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mkdirs: " + path.toString() + " permission: " + fsPermission.toShort());
        }
        statistics.incrementWriteOps(1);
        try {
            FsPermission umask = FsPermission.getUMask(getConf());
            short perm = fsPermission.applyUMask(umask).toShort();
            return storage.mkdirs(parsePath(path), perm, uid, gid);
        } catch (Exception e) {
            log.error("Failed to mkdirs:" + path.toString());
            throw new IOException(e);
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("getFileStatus:" + path.toString());
        }
        try {
            String pathStr = parsePath(path);
            CfsStatInfo info = storage.stat(pathStr);
            if (info == null) {
                throw new FileNotFoundException(path.toString());
            }
            return FileStatusHelper.convert(storage, (uri == null ? null : uri.toString()), pathStr, info);
        } catch (CfsException ex) {
            throw new IOException(ex);
        }
    }

    //parse the Path is Cfs
    private String parsePath(Path p) {
    /*
    if (log.isDebugEnabled()) {
      log.debug("path:" + p.toString());
    }
     */
        Path path = makeQualified(p);
        URI pathURI = path.toUri();
        if (pathURI == null) {
            throw new IllegalArgumentException("Couldn't parse the path, " + path);
        }

        String res = null;
        String scheme = pathURI.getScheme();
        if (scheme != null) {
            if (scheme.equals(CFS_SCHEME_NAME) == false) {
                throw new IllegalArgumentException("Not support the scheme, from the path, " + path);
            }

            res = pathURI.getPath().trim();
            if (res == null || res.isEmpty()) {
                throw new IllegalArgumentException(path + " is invalid.");
            }
        } else {
            res = pathURI.toString().trim();
            if (res == null || res.isEmpty()) {
                log.warn("Reset the path with working-dir: " + workingDir);
                res = workingDir.toString();
            } else if (path.isAbsolute() == false) {
                res = new Path(workingDir.toString() + path.toString()).toString();
            } else {
                res = path.toString();
            }
        }

        if (res.startsWith("/") == false) {
            throw new IllegalArgumentException(path + " is invalid.");
        }

        return res;
    }

    @Override
    public FsStatus getStatus() throws IOException {
        throw new IOException("Not implement getStatus.");
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        if (!flags.contains(CreateFlag.CREATE)) {
            throw new IOException("Not support the flags:" + flags.toString());
        }

        String pathStr = parsePath(path);
        CfsStatInfo info = null;
        try {
            info = storage.stat(pathStr);
        } catch (CfsException e) {
            throw new IOException(e);
        }

        int fls = FileStorage.O_WRONLY;
        if (flags.size() == 1) {
            fls = fls | FileStorage.O_CREAT;
        } else if ((flags.size() == 2) && (flags.contains(CreateFlag.OVERWRITE))) {
            if (info == null) {
                fls = FileStorage.O_WRONLY | FileStorage.O_CREAT;
            } else if (info.getType() != CfsStatInfo.Type.DIR) {
                fls = FileStorage.O_WRONLY | FileStorage.O_TRUNC;
            } else {
                throw new IOException("The path: " + path.toString() + " is a dir.");
            }
        } else {
            throw new IOException("Invalid flags:" + flags.toString());
        }

        statistics.incrementWriteOps(1);
        CfsFile cfile = null;
        try {
            cfile = storage.open(pathStr, fls, cfg.CFS_DEFAULT_FILE_PERMISSION, uid, gid);
        } catch (CfsException ex) {
            log.error("Failed to create:" + path.toString());
            throw new IOException(ex);
        }
        CfsDataOutputStream output = new CfsDataOutputStream(cfile);
        return new FSDataOutputStream(output, statistics);
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("setPermission:" + path.toString() + " permission:" + permission);
        }
        try {
            storage.chmod(parsePath(path), permission.toShort());
        } catch (CfsException ex) {
            log.error(ex.getMessage(), ex);
            throw new IOException(ex);
        }
    }

    @Override
    public void setOwner(Path path, String username, String groupname) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("setOwner:" + path.toString() + " username:" + username + " groupname:" + groupname);
        }
        try {
            storage.chown(parsePath(path), username, groupname);
        } catch (CfsException ex) {
            log.error(ex.getMessage(), ex);
            throw new IOException(ex);
        }
    }

    @Override
    public void setTimes(Path path, long mtime, long atime) throws IOException {
        if (mtime < 0) {
            mtime = 0L;
        }
        if (atime < 0) {
            atime = 0L;
        }

        if (mtime > 0) {
            mtime = mtime * 1000;
        }

        if (atime > 0) {
            atime = atime * 1000;
        }

        if (log.isDebugEnabled()) {
            log.debug("setTimes:" + path.toString() + " mtime:" + mtime + " atime:" + atime);
        }
        log.info("setTimes:" + path.toString() + " mtime:" + mtime + " atime:" + atime);
        try {
            storage.setTimes(parsePath(path), mtime, atime);
        } catch (CfsException ex) {
            log.error(ex.getMessage(), ex);
            throw new IOException(ex);
        }
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        throw new IOException("Unimplement getFileCheckSum:  " + f.toString() + ".");
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        throw new IOException("Unsupport getAclStatus: " + path.toString() + ".");
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        throw new IOException("Unsupport setAcl: " + path.toString() + ".");
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        throw new IOException("Unsupport removeAclEntryes: " + path.toString() + ".");
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        throw new IOException("Not implement getXAttr:  " + path.toString() + ".");
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        throw new IOException("Not implement getXAttrs:  " + path.toString() + ".");
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
        throw new IOException("Not implement getXAttrs:  " + path.toString() + ".");
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        throw new IOException("Not implement listXAttrs:  " + path.toString() + ".");
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value) throws IOException {
        throw new IOException("Not implement setXAttrs:  " + path.toString() + ".");
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
        throw new IOException("Not implement setXAttrs:  " + path.toString() + ".");
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        throw new IOException("Not implement removeXAttrs:  " + path.toString() + ".");
    }

   /* private String validPath(Path path) {
        log.info("path:" + path.toString());
        URI pathURI = path.toUri();
        log.info("URI.getPath:" + pathURI.getPath());
        if (pathURI == null) {
            throw new IllegalArgumentException("Couldn't parse the path, " + path);
        }

        String res = null;
        String scheme = pathURI.getScheme();
        if (scheme != null) {
            if (scheme.equals(CFS_SCHEME_NAME) == false) {
                throw new IllegalArgumentException("Not support the scheme, from the path, " + path);
            }

            res = pathURI.getPath().trim();
            if (res == null || res.isEmpty()) {
                throw new IllegalArgumentException(path + " is invalid.");
            }
        } else {
            res = pathURI.toString().trim();
            if (res == null || res.isEmpty()) {
                log.warn("Reset the path with working-dir: " + workingDir);
                res = workingDir.toString();
            } else if (path.isAbsolute() == false) {
                res = new Path(workingDir.toString() + path.toString()).toString();
            } else {
                res = path.toString();
            }
        }

        if (res.startsWith("/") == false) {
            throw new IllegalArgumentException(path + " is invalid.");
        }

        return res;
    }*/





}
