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
package org.apache.camel.component.kafka.consumer.errorhandler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.component.kafka.KafkaFetchRecords;
import org.apache.camel.support.BridgeExceptionHandlerToErrorHandler;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Poll exceptions that aren't about a record must not skip records or end the consumer.
public class PollErrorRecoveryTest {
    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final TopicPartition P1 = new TopicPartition("orders", 1);

    private MockConsumer<Object, Object> consumer;
    private KafkaFetchRecords fetcher;
    private BridgeExceptionHandlerToErrorHandler bridge;

    @BeforeEach
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(Arrays.asList(P0, P1));
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(P0, 0L);
        beginning.put(P1, 0L);
        consumer.updateBeginningOffsets(beginning);
        consumer.seek(P0, 10);
        consumer.seek(P1, 20);

        fetcher = mock(KafkaFetchRecords.class);
        bridge = mock(BridgeExceptionHandlerToErrorHandler.class);
        when(fetcher.getBridge()).thenReturn(bridge);
    }

    private void assertPositions(long p0, long p1) {
        assertEquals(p0, consumer.position(P0), "orders-0 position");
        assertEquals(p1, consumer.position(P1), "orders-1 position");
    }

    @Test
    public void errorHandlerKeepsConsumingAfterTransientErrorsWithoutSkipping() {
        BridgeErrorStrategy strategy = new BridgeErrorStrategy(fetcher, consumer);
        for (Exception e : new Exception[] {
                new SaslAuthenticationException("token endpoint down"),
                new AuthorizationException("not yet"),
                new DisconnectException("broker gone"),
                new TimeoutException("slow") }) {
            strategy.handle(-1, e);
            assertTrue(strategy.canContinue(), e.getClass().getSimpleName() + " ended the consumer");
            verify(bridge).handleException(e);
        }
        assertPositions(10, 20);
        verify(fetcher, never()).setReconnect(anyBoolean());
    }

    @Test
    public void errorHandlerSkipsOnlyTheRecordThatCantBeDeserialized() {
        BridgeErrorStrategy strategy = new BridgeErrorStrategy(fetcher, consumer);
        strategy.handle(-1, new RecordDeserializationException(P1, 20, "bad record", new RuntimeException()));
        assertTrue(strategy.canContinue());
        assertPositions(10, 21);
    }

    @Test
    public void errorHandlerRecreatesAFencedConsumer() {
        BridgeErrorStrategy strategy = new BridgeErrorStrategy(fetcher, consumer);
        strategy.handle(-1, new FencedInstanceIdException("fenced"));
        assertTrue(strategy.canContinue());
        verify(fetcher).setReconnect(true);
        verify(fetcher).setConnected(false);
        assertPositions(10, 20);
    }

    @Test
    public void discardSkipsOnlyTheRecordThatCantBeDeserialized() {
        DiscardErrorStrategy strategy = new DiscardErrorStrategy(consumer);
        strategy.handle(-1, new DisconnectException("broker gone"));
        assertPositions(10, 20);

        strategy.handle(-1, new RecordDeserializationException(P0, 10, "bad record", new RuntimeException()));
        assertPositions(11, 20);
    }
}
