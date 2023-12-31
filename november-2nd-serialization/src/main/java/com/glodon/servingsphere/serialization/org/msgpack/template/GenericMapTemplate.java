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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class GenericMapTemplate implements GenericTemplate {
    @SuppressWarnings("rawtypes")
    Constructor<? extends Template> constructor;

    @SuppressWarnings("rawtypes")
    public GenericMapTemplate(TemplateRegistry registry, Class<? extends Template> tmpl) {
        try {
            constructor = tmpl.getConstructor(new Class<?>[] { Template.class, Template.class });
            constructor.newInstance(new Object[] { new AnyTemplate(registry), new AnyTemplate(registry) });
            // AnyTemplate.getInstance(registry),
            // AnyTemplate.getInstance(registry)});
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    public Template build(Template[] params) {
        try {
            return constructor.newInstance((Object[]) params);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
