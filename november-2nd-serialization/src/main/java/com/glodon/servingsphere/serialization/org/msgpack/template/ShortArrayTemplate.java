//
// MessagePack for Java
//
// Copyright (C) 2009 - 2013 FURUHASHI Sadayuki
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
package com.glodon.servingsphere.serialization.org.msgpack.template;

import com.glodon.servingsphere.serialization.org.msgpack.MessageTypeException;
import com.glodon.servingsphere.serialization.org.msgpack.packer.Packer;
import com.glodon.servingsphere.serialization.org.msgpack.unpacker.Unpacker;

import java.io.IOException;

public class ShortArrayTemplate extends AbstractTemplate<short[]> {
    private ShortArrayTemplate() {
    }

    public void write(Packer pk, short[] target, boolean required)
            throws IOException {
        if (target == null) {
            if (required) {
                throw new MessageTypeException("Attempted to write null");
            }
            pk.writeNil();
            return;
        }
        pk.writeArrayBegin(target.length);
        for (short a : target) {
            pk.write(a);
        }
        pk.writeArrayEnd();
    }

    public short[] read(Unpacker u, short[] to, boolean required)
            throws IOException {
        if (!required && u.trySkipNil()) {
            return null;
        }
        int n = u.readArrayBegin();
        if (to == null || to.length != n) {
            to = new short[n];
        }
        for (int i = 0; i < n; i++) {
            to[i] = u.readShort();
        }
        u.readArrayEnd();
        return to;
    }

    static public ShortArrayTemplate getInstance() {
        return instance;
    }

    static final ShortArrayTemplate instance = new ShortArrayTemplate();
}
