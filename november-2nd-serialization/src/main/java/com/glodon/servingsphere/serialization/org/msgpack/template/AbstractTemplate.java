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

import com.glodon.servingsphere.serialization.org.msgpack.packer.Packer;
import com.glodon.servingsphere.serialization.org.msgpack.unpacker.Unpacker;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

public abstract class AbstractTemplate<T> implements Template<T> {

    private static final Unsafe THE_UNSAFE;
    static
    {
        try
        {
            final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>()
            {
                public Unsafe run() throws Exception
                {
                    Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                    theUnsafe.setAccessible(true);
                    return (Unsafe) theUnsafe.get(null);
                }
            };

            THE_UNSAFE = AccessController.doPrivileged(action);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to load unsafe", e);
        }
    }

    protected Unsafe getUnsafe()
    {
        return THE_UNSAFE;
    }

    public void write(Packer pk, T v) throws IOException {
        write(pk, v, false);
    }

    public T read(Unpacker u, T to) throws IOException {
        return read(u, to, false);
    }
}
