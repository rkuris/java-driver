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
import static org.testng.Assert.fail;

import com.datastax.driver.core.AbstractPoliciesTest;
import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

import static com.datastax.driver.core.TestUtils.waitForDownWithWait;


public class DowngradingConsistencyRetryTest extends AbstractPoliciesTest{

    /**
     * Tests DowngradingConsistencyRetryPolicy with LoggingRetryPolicy
     */
    @Test(groups = "long")
    public void downgradingConsistencyLoggingPolicy() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy())
            .withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE));
        downgradingConsistencyRetryPolicy(builder);
    }

    /**
     * Tests DowngradingConsistencyRetryPolicy
     */
    public void downgradingConsistencyRetryPolicy(Cluster.Builder builder) throws Throwable {
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {
            createSchema(c.session, 3);
            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers
            Thread.sleep(5000);

            init(c, 12, ConsistencyLevel.ALL);
            query(c, 12, ConsistencyLevel.ALL);

            assertQueried(CCMBridge.IP_PREFIX + '1', 4);
            assertQueried(CCMBridge.IP_PREFIX + '2', 4);
            assertQueried(CCMBridge.IP_PREFIX + '3', 4);

            resetCoordinators();
            c.cassandraCluster.stop(2);
            waitForDownWithWait(CCMBridge.IP_PREFIX + '2', c.cluster, 10);

            query(c, 12, ConsistencyLevel.ALL);

            assertQueried(CCMBridge.IP_PREFIX + '1', 6);
            assertQueried(CCMBridge.IP_PREFIX + '2', 0);
            assertQueried(CCMBridge.IP_PREFIX + '3', 6);

            resetCoordinators();
            c.cassandraCluster.stop(1);
            waitForDownWithWait(CCMBridge.IP_PREFIX + '1', c.cluster, 10);

            try {
                query(c, 12, ConsistencyLevel.ALL);
            } catch (ReadTimeoutException e) {
                assertEquals("Cassandra timeout during read query at consistency TWO (2 responses were required but only 1 replica responded)", e.getMessage());
            }

            Thread.sleep(15000);
            resetCoordinators();

            try {
                query(c, 12, ConsistencyLevel.TWO);
            } catch (Exception e) {
                fail("Only 1 node is up and CL.TWO should downgrade and pass.");
            }

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 0);
            assertQueried(CCMBridge.IP_PREFIX + '3', 12);

            resetCoordinators();

            try {
                query(c, 12, ConsistencyLevel.ALL);
            } catch (Exception e) {
                fail("Only 1 node is up and CL.ALL should downgrade and pass.");
            }

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 0);
            assertQueried(CCMBridge.IP_PREFIX + '3', 12);

            resetCoordinators();

            query(c, 12, ConsistencyLevel.QUORUM);

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 0);
            assertQueried(CCMBridge.IP_PREFIX + '3', 12);

            resetCoordinators();

            query(c, 12, ConsistencyLevel.TWO);

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 0);
            assertQueried(CCMBridge.IP_PREFIX + '3', 12);

            resetCoordinators();

            query(c, 12, ConsistencyLevel.ONE);

            assertQueried(CCMBridge.IP_PREFIX + '1', 0);
            assertQueried(CCMBridge.IP_PREFIX + '2', 0);
            assertQueried(CCMBridge.IP_PREFIX + '3', 12);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

}
