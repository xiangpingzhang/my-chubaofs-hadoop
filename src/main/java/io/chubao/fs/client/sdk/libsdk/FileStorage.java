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

import io.chubao.fs.client.sdk.client.CfsFile;
import io.chubao.fs.client.sdk.exception.CfsException;

import java.util.List;
import java.util.Map;

public interface FileStorage {
    int O_RDONLY = 0;
    int O_WRONLY = 1;
    int O_ACCMODE = 3;
    int O_CREAT = 64;
    int O_TRUNC = 512;
    int O_APPEND = 1024;

    int S_IFDIR = 16384;
    int S_IFREG = 32768;
    int S_IFLNK = 40960;

    boolean mkdirs(String path, int mode, int uid, int gid) throws CfsException;

    CfsFile open(String path, int flags, int mode, int uid, int gid) throws CfsException;

    void truncate(String path, long newLength) throws CfsException;

    void close() throws CfsException;

    void rmdir(String path, boolean recursive) throws CfsException;

    void unlink(String path) throws CfsException;

    void rename(String src, String dst) throws CfsException;

    CfsStatInfo[] list(String path) throws CfsException;

    CfsStatInfo stat(String path) throws CfsException;

    void setXAttr(String path, String name, byte[] value) throws CfsException;

    byte[] getXAttr(String path, String name) throws CfsException;

    List<String> listXAttr(String path) throws CfsException;

    Map<String, byte[]> getXAttrs(String path, List<String> names) throws CfsException;

    void removeXAttr(String path, String name) throws CfsException;

    void chown(String path, int uid, int gid) throws CfsException;

    void chown(String path, String user, String group) throws CfsException;

    void chmod(String path, int mode) throws CfsException;

    void setTimes(String path, long mtime, long atime) throws CfsException;

    long getBlockSize();

    int getReplicaNumber();

    int getUid(String username) throws CfsException;

    int getGid(String groupname) throws CfsException;

    int getGidByUser(String user) throws CfsException;

    String getUser(int uid) throws CfsException;

    String getGroup(int gid) throws CfsException;
}
