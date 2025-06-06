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
package org.apache.camel.component.sql;

import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlDynamicKameletTest extends CamelTestSupport {

    EmbeddedDatabase db;

    @Override

    public void doPreSetup() throws Exception {
        db = new EmbeddedDatabaseBuilder()
                .setName(getClass().getSimpleName())
                .setType(EmbeddedDatabaseType.H2)
                .addScript("sql/createAndPopulateDatabase.sql").build();
    }

    @Override
    public void doPostTearDown() throws Exception {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    public void testSimulateDynamicKamelet() throws InterruptedException {
        MockEndpoint mock = getMockEndpoint("mock:query");
        mock.expectedMessageCount(1);

        template.requestBodyAndHeader("direct:query", "ASF", "names", "Camel,AMQ");

        MockEndpoint.assertIsSatisfied(context);

        List list = mock.getReceivedExchanges().get(0).getIn().getBody(List.class);
        assertEquals(2, list.size());
        Map row = (Map) list.get(0);
        assertEquals("Camel", row.get("PROJECT"));
        row = (Map) list.get(1);
        assertEquals("AMQ", row.get("PROJECT"));
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // required for the sql component
                getContext().getComponent("sql", SqlComponent.class).setDataSource(db);

                context.getPropertiesComponent().addInitialProperty("localSql", "sql");
                context.getPropertiesComponent().addInitialProperty("myQuery", "classpath:sql/selectProjectsAndIn.sql");

                from("direct:query")
                        .to("{{localSql}}?query={{myQuery}}")
                        .to("log:query")
                        .to("mock:query");
            }
        };
    }
}
