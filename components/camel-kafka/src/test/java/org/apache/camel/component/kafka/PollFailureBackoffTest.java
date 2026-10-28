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
package org.apache.camel.component.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PollFailureBackoffTest {

    // random 0.5 is the middle of the +/-20% jitter, so no jitter
    private static long backoff(int failures) {
        return KafkaFetchRecords.pollFailureBackoffMs(1000, 60000, failures, 0.5);
    }

    @Test
    public void doublesPerConsecutiveFailureUpToTheMax() {
        assertEquals(1000, backoff(1));
        assertEquals(2000, backoff(2));
        assertEquals(4000, backoff(3));
        assertEquals(32000, backoff(6));
        assertEquals(60000, backoff(7));
        assertEquals(60000, backoff(1000));
    }

    @Test
    public void jitterIsPlusOrMinusTwentyPercent() {
        assertEquals(800, KafkaFetchRecords.pollFailureBackoffMs(1000, 60000, 1, 0.0));
        assertEquals(1200, KafkaFetchRecords.pollFailureBackoffMs(1000, 60000, 1, 1.0));
        assertEquals(48000, KafkaFetchRecords.pollFailureBackoffMs(1000, 60000, 50, 0.0));
    }
}
