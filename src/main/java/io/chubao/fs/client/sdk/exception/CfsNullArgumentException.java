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


import static io.chubao.fs.client.sdk.exception.StatusCodes.CFS_STATUS_NULL_ARGUMENT;

public class CfsNullArgumentException extends CfsException {
    public CfsNullArgumentException(String msg) {
        super(msg, CFS_STATUS_NULL_ARGUMENT.code());
    }
}
