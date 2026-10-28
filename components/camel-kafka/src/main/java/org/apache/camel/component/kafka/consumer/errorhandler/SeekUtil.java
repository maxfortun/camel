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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SeekUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SeekUtil.class);

    private SeekUtil() {

    }

    /**
     * Skips the record a poll failed on, if the exception names one. Only a record that can't be deserialized stops
     * the consumer from moving past it; it is skipped on its own partition. Any other poll exception (network,
     * timeout, authentication, fencing) is not about a record, so nothing is skipped and the next poll retries.
     *
     * @return whether a record was skipped
     */
    static boolean seekPastFailedRecord(Consumer<?, ?> consumer, Exception exception) {
        if (!(exception instanceof RecordDeserializationException)) {
            LOG.debug("Not skipping any record: {} is not about a record", exception.getClass().getName());
            return false;
        }
        RecordDeserializationException e = (RecordDeserializationException) exception;
        TopicPartition tp = e.topicPartition();
        long next = e.offset() + 1;
        LOG.warn("Skipping the record that can't be deserialized: seeking to offset {} on topic {} partition {}",
                next, tp.topic(), tp.partition());
        consumer.seek(tp, next);
        return true;
    }
}
