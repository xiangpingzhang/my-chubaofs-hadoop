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

package io.chubao.fs.client.sdk.exception;

public class CfsException extends  Exception{
    private static final long serialVersionUID = 1L;

    protected  int code;

    public CfsException(String msg) {
        super(msg);
    }

    public CfsException(String msg, int code) {
        super(msg);
        this.code = code;
    }

    public CfsException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public CfsException(String msg, Throwable cause, int code) {
        super(msg, cause);
        this.code = code;
    }

    public CfsException(Throwable cause) {
        super(cause);
    }

    public CfsException(Throwable cause, int code) {
        super(cause);
        this.code = code;
    }

    public int getErrorCode() {
        return this.code;
    }
}
