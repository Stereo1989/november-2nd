//
// MessagePack for Java
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.glodon.servingsphere.serialization.org.msgpack.unpacker;

import java.io.IOException;

@SuppressWarnings("serial")
public class SizeLimitException extends IOException {
    public SizeLimitException() {
        super();
    }

    public SizeLimitException(String message) {
        super(message);
    }

    public SizeLimitException(String message, Throwable cause) {
        super(message, cause);
    }

    public SizeLimitException(Throwable cause) {
        super(cause);
    }
}
