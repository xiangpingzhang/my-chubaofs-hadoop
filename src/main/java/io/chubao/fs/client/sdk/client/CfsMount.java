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
package io.chubao.fs.client.sdk.client;

import com.sun.jna.Native;
import io.chubao.fs.client.sdk.libsdk.FileStorage;
import io.chubao.fs.client.sdk.libsdk.FileStorageImpl;
import io.chubao.fs.client.config.StorageConfig;
import io.chubao.fs.client.sdk.exception.CfsException;
import io.chubao.fs.client.sdk.exception.CfsNullArgumentException;
import io.chubao.fs.client.sdk.exception.StatusCodes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;

public class CfsMount {
    private static final Log log = LogFactory.getLog(CfsMount.class);
    //private static AtomicLong clientID = new AtomicLong(0L);
    private CfsLibrary libCfs;
    private String sdkLibPath;
    private long cid;

    public CfsMount(String libpath) throws CfsException{
        this.sdkLibPath = libpath;
        if (sdkLibPath == null) {
            throw new CfsNullArgumentException("Please specify the libsdk.so path.");
        }
        File file = new File(sdkLibPath);
        if (file.exists() == false) {
            throw new CfsNullArgumentException("Not found the libsdk.so: " + sdkLibPath);
        }
        libCfs = Native.load(sdkLibPath, CfsLibrary.class);
        this.cid = libCfs.cfs_new_client();
        if (cid < 0) {
            throw new CfsException("Failed to new a client.");
        }

    }

    public FileStorage openFileStorage(StorageConfig config,CfsMount mnt) throws CfsException {
        libCfs.cfs_set_client(mnt.cid, StorageConfig.CONFIG_KEY_MATSER, config.getMasters());
        libCfs.cfs_set_client(mnt.cid, StorageConfig.CONFIG_KEY_VOLUME, config.getVolumeName());
        libCfs.cfs_set_client(mnt.cid, StorageConfig.CONFIG_KEY_LOG_DIR, config.getLogDir());
        libCfs.cfs_set_client(mnt.cid, StorageConfig.CONFIG_KEY_LOG_LEVEL, config.getLogLevel());

        int st = libCfs.cfs_start_client(mnt.cid);

        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to start the client: " + mnt.cid + " status code: " + st);
        }

        st = libCfs.cfs_chdir(cid, "/");
        if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
            throw new CfsException("Failed to chdir for client: " + mnt.cid + " status code: " + st);
        }

        try {
            //CfsDriverIns ins = new CfsDriverIns(driver, cid);
            FileStorageImpl storage = new FileStorageImpl(libCfs,mnt.cid);
            storage.init();
            log.info("Success to open FileStorage, client id:" + mnt.cid);
            return storage;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
