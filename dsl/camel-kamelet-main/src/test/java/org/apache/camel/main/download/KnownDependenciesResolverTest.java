/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.main.download;

import org.apache.camel.impl.engine.SimpleCamelContext;
import org.apache.camel.tooling.maven.MavenGav;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KnownDependenciesResolverTest {

    @Test
    void mavenGavForClass_returnsClassScopedDependency() {
        KnownDependenciesResolver resolver = new KnownDependenciesResolver(new SimpleCamelContext(), null, null);
        resolver.loadKnownDependencies();

        MavenGav dependency = resolver.mavenGavForClass(SomeClass.class.getName());

        assertNotNull(dependency);
        assertEquals(dependency.getGroupId(), "com.example");
        assertEquals(dependency.getArtifactId(), "class-scoped");
        assertEquals(dependency.getVersion(), "1.0.0");
    }

    @Test
    void mavenGavForClass_returnsPackageScopedDependency() {
        KnownDependenciesResolver resolver = new KnownDependenciesResolver(new SimpleCamelContext(), null, null);
        resolver.loadKnownDependencies();

        MavenGav dependency = resolver.mavenGavForClass(SomeClass.class.getPackage().getName());

        assertNotNull(dependency);
        assertEquals(dependency.getGroupId(), "org.example");
        assertEquals(dependency.getArtifactId(), "package-scoped");
        assertEquals(dependency.getVersion(), "2.0.0");
    }

    public static class SomeClass {
    }
}
