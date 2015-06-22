/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;

public class RetryDecisionTest {

    /**
     * Test RetryPolicy to ensure the basic unit get tests for the RetryDecisions.
     */
    public static class TestRetryPolicy implements RetryPolicy {
        public RetryDecision onReadTimeout(Statement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return RetryDecision.rethrow();
        }

        public RetryDecision onWriteTimeout(Statement query, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return RetryDecision.rethrow();
        }

        public RetryDecision onUnavailable(Statement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return RetryDecision.rethrow();
        }

        public static void testRetryDecision() {
            assertEquals(RetryDecision.retry(ConsistencyLevel.ONE).getType(), RetryDecision.Type.RETRY);
            assertEquals(RetryDecision.retry(ConsistencyLevel.ONE).getRetryConsistencyLevel(), ConsistencyLevel.ONE);
            assertEquals(RetryDecision.rethrow().getType(), RetryDecision.Type.RETHROW);
            assertEquals(RetryDecision.ignore().getType(), RetryDecision.Type.IGNORE);

            assertEquals(RetryDecision.retry(ConsistencyLevel.ONE).toString(), "Retry at " + ConsistencyLevel.ONE + " on same host.");
            assertEquals(RetryDecision.tryNextHost(ConsistencyLevel.ONE).toString(), "Retry at " + ConsistencyLevel.ONE + " on next host.");
            assertEquals(RetryDecision.rethrow().toString(), "Rethrow");
            assertEquals(RetryDecision.ignore().toString(), "Ignore");
        }
    }

    /**
     * Test RetryDecision get variables and defaults are correct.
     */
    @Test(groups = "unit")
    public void RetryDecisionTest() throws Throwable {
        TestRetryPolicy.testRetryDecision();
    }
}
