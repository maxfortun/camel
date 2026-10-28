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

import org.apache.camel.component.kafka.KafkaFetchRecords;
import org.apache.camel.component.kafka.PollExceptionStrategy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes poll exceptions to the error handler and keeps consuming:
 * <ul>
 * <li>a record that can't be deserialized is skipped, on its own partition;</li>
 * <li>a consumer fenced by another one with the same group.instance.id is closed and created again;</li>
 * <li>anything else (network, timeout, authentication, authorization) keeps the consumer, and the next poll retries.
 * Authentication and authorization errors can be transient, e.g. a token endpoint or a broker's SASL handler
 * failing during an outage, and ending the consumer thread on them leaves the route started but consuming
 * nothing.</li>
 * </ul>
 * KafkaFetchRecords backs off before polling again after a failed poll.
 */
public class BridgeErrorStrategy implements PollExceptionStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeErrorStrategy.class);
    private final KafkaFetchRecords recordFetcher;
    private final Consumer<?, ?> consumer;

    public BridgeErrorStrategy(KafkaFetchRecords recordFetcher, Consumer<?, ?> consumer) {
        this.recordFetcher = recordFetcher;
        this.consumer = consumer;
    }

    @Override
    public boolean canContinue() {
        return true;
    }

    @Override
    public void handle(long partitionLastOffset, Exception exception) {
        LOG.warn("Deferring processing to the exception handler based on polling exception strategy");

        // use bridge error handler to route with exception
        recordFetcher.getBridge().handleException(exception);

        if (exception instanceof FencedInstanceIdException) {
            // A fenced consumer fails every poll from now on; a new one takes the group.instance.id back
            LOG.warn("Consumer was fenced by another consumer with the same group.instance.id; creating it again");
            recordFetcher.setReconnect(true);
            recordFetcher.setConnected(false);
            return;
        }

        SeekUtil.seekPastFailedRecord(consumer, exception);
    }
}
