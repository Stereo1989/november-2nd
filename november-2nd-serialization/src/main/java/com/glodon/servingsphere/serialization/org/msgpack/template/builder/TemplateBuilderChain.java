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
package com.glodon.servingsphere.serialization.org.msgpack.template.builder;

import com.glodon.servingsphere.serialization.org.msgpack.template.TemplateRegistry;
import com.glodon.servingsphere.serialization.org.msgpack.util.android.DalvikVmChecker;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TemplateBuilderChain {

    private static final Logger LOG = Logger.getLogger(TemplateBuilderChain.class.getName());

    private static final String JAVASSIST_TEMPLATE_BUILDER_CLASS_NAME =
            "com.glodon.servingsphere.serialization.org.msgpack.template.builder.JavassistTemplateBuilder";
    private static final String REFLECTION_TEMPLATE_BUILDER_CLASS_NAME =
            "com.glodon.servingsphere.serialization.org.msgpack.template.builder.ReflectionTemplateBuilder";

    private static boolean enableDynamicCodeGeneration() {
        return !DalvikVmChecker.isDalvikVm() &&
                Boolean.parseBoolean(System.getProperties().getProperty("msgpack.dynamic-codegen.enabled", "true"));
    }

    protected List<TemplateBuilder> templateBuilders;

    protected TemplateBuilder forceBuilder;

    public TemplateBuilderChain(final TemplateRegistry registry) {
        this(registry, null);
    }

    public TemplateBuilderChain(final TemplateRegistry registry, final ClassLoader cl) {
        templateBuilders = new ArrayList<TemplateBuilder>();
        reset(registry, cl);
    }

    protected void reset(final TemplateRegistry registry, final ClassLoader cl) {
        if (registry == null) {
            throw new NullPointerException("registry is null");
        }

        // FIXME
        // Javassist{,Beans}TemplateBuilder should be created with reflection for android.

        String forceBuilderClassName = null;
        if (enableDynamicCodeGeneration()) { // use dynamic code generation
            forceBuilderClassName = JAVASSIST_TEMPLATE_BUILDER_CLASS_NAME;
        } else {
            forceBuilderClassName = REFLECTION_TEMPLATE_BUILDER_CLASS_NAME;
        }

        // create builder chain
        forceBuilder = createForceTemplateBuilder(forceBuilderClassName, registry, cl);
        TemplateBuilder builder = forceBuilder;
        templateBuilders.add(new ArrayTemplateBuilder(registry));
        templateBuilders.add(new OrdinalEnumTemplateBuilder(registry));
        templateBuilders.add(builder);
        templateBuilders.add(new ReflectionBeansTemplateBuilder(registry));
    }

    private static TemplateBuilder createForceTemplateBuilder(String className,
                                                              TemplateRegistry registry, ClassLoader cl) {
        try {
            Class<?> c = (Class<?>) Class.forName(className);
            Constructor<?> cons = c.getConstructor(TemplateRegistry.class,
                    ClassLoader.class);
            return (TemplateBuilder) cons.newInstance(registry, cl);
        } catch (Exception e) {
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.log(Level.WARNING, "Failed to create a TemplateBuilder reflectively", e);
            }
        }
        return new ReflectionTemplateBuilder(registry, cl);
    }

    public TemplateBuilder getForceBuilder() {
        return forceBuilder;
    }

    public TemplateBuilder select(final Type targetType, final boolean hasAnnotation) {
        for (TemplateBuilder tb : templateBuilders) {
            if (tb.matchType(targetType, hasAnnotation)) {
                return tb;
            }
        }
        return null;
    }
}
