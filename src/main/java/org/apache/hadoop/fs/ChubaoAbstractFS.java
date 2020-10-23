package org.apache.hadoop.fs;

import org.apache.commons.logging.Log;
import io.chubao.fs.client.config.CfsConfig;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ChubaoFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

public class ChubaoAbstractFS extends AbstractFileSystem{
    private static final Log log = LogFactory.getLog(ChubaoAbstractFS.class);
    private ChubaoFileSystem cfs;
    private boolean verifyCheckSum =false;
    private FsServerDefaults fsServer;




    public ChubaoAbstractFS(final URI theUri, final Configuration conf) throws IOException, URISyntaxException  {
        super(theUri, CfsConfig.CFS_SCHEME_NAME,true,CfsConfig.CFS_DEFAULT_PORT);
        log.info("==> Initialize ChuaoAbstractFS");

        if (!theUri.getScheme().equalsIgnoreCase(CfsConfig.CFS_SCHEME_NAME)) {
            throw new IllegalArgumentException(theUri.toString() + " is not ChubaoFS scheme, Unable to initialize ChubaoFS");
        }

        String host = theUri.getHost();
        if (host == null) {
            throw new IOException("Incomplete ChubaoFS URI, no host: " + theUri);
        }

        cfs = new ChubaoFileSystem();

        cfs.initialize(theUri, conf);

        fsServer = new FsServerDefaults(
                64 * 1024 * 1024,
                1,
                64 * 1024 * 1024,
                (short) 3,
                64 * 1024 * 1024,
                false,
                3600,
                org.apache.hadoop.util.DataChecksum.Type.NULL);
    }

    @Override
    public int getUriDefaultPort() {
        return CfsConfig.CFS_DEFAULT_PORT;
    }

    protected  void finalize() throws Throwable{
        try{
            cfs.close();
        }finally{
            super.finalize();
        }
    }

    @Override
    public boolean truncate(Path f, long newLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return cfs.truncate(f, newLength);
    }

    @Override
    public boolean supportsSymlinks() {
        return true;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return fsServer;
    }


    @Override
    public FSDataOutputStream createInternal(Path f, EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, UnresolvedLinkException, IOException {
        if (createParent) {
            try {
                cfs.mkdirs(f.getParent(), absolutePermission);
            } catch (IOException e) {
                throw e;
            }
        }
        return cfs.create(f, absolutePermission, flag, bufferSize, replication, blockSize, progress, checksumOpt);
    }


    @Override
    public void mkdir(Path path, FsPermission fsPermission, boolean creatParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException, IOException {
        if (log.isDebugEnabled()) {
            log.debug("mkdir: " + path.toString());
        }
        if (creatParent) {
            try {
                cfs.mkdirs(path.getParent(), fsPermission);
            } catch (IOException e) {
                throw e;
            }
        }
        boolean result = cfs.mkdirs(path, fsPermission);
        if (!result) {
            throw new IOException("Failed to mkdir: " + path.toString());
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return cfs.delete(path, recursive);
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return cfs.open(path,i);
    }

    @Override
    public boolean setReplication(Path path, short i) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return false;
    }

    @Override
    public void renameInternal(Path src, Path dst) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException
    {
        if (cfs.rename(src, dst) == false) {
            throw new IOException("Failed to rename:" + src.toString() + " to:" + dst.toString());
        }
    }

    @Override
    public void setPermission(Path path, FsPermission fsPermission) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

    }

    @Override
    public void setOwner(Path path, String username, String groupname) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        cfs.setOwner(path, username, groupname);
    }

    @Override
    public void setTimes(Path path, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        cfs.setTimes(path, mtime, atime);
    }

    @Override
    public FileChecksum getFileChecksum(Path path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return cfs.getFileChecksum(path, Long.MAX_VALUE);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return cfs.getFileStatus(path);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return cfs.getFileBlockLocations(path, start, len);
    }

    @Override
    public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
        return cfs.getStatus();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return cfs.listStatus(path);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {
        this.verifyCheckSum = verifyChecksum;
    }

}
