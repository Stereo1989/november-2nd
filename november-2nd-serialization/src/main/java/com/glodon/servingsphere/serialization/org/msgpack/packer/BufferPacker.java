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
package com.glodon.servingsphere.serialization.org.msgpack.packer;

/**
 * This class is buffer-specific serializer.
 * 
 * @version 0.6.0
 * @see {@link Packer}
 */
public interface BufferPacker extends Packer {
    public int getBufferSize();

    public byte[] toByteArray();

    public void clear();
}
