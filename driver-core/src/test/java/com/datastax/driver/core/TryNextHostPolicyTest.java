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
package com.datastax.driver.core;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.primitives.UnsignedBytes;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import static com.datastax.driver.core.Assertions.assertThat;

public class TryNextHostPolicyTest {

    SCassandraCluster scassandras;

    Cluster cluster = null;
    Metrics.Errors errors;
    Host host1, host2, host3;
    Session session;

    @BeforeClass(groups = "short")
    public void beforeClass() {
        scassandras = new SCassandraCluster(CCMBridge.IP_PREFIX, 3);
    }

    @BeforeMethod(groups = "short")
    public void beforeMethod() {

        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.ipOfNode(1))
            .withRetryPolicy(new TryNextHostOnUnavailableRetryPolicy())
            .withLoadBalancingPolicy(new SortingLoadBalancingPolicy())
            .build();

        session = cluster.connect();

        host1 = TestUtils.findHost(cluster, 1);
        host2 = TestUtils.findHost(cluster, 2);
        host3 = TestUtils.findHost(cluster, 3);

        errors = cluster.getMetrics().getErrorMetrics();
    }

    @Test(groups = "short")
    public void should_try_on_next_host_in_query_plan_retry_policy_test() {
        scassandras.prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.unavailable)
                .build()
        );
        SimpleStatement statement = new SimpleStatement("mock query");
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        ResultSet rs = session.execute(statement);;
        List<Host> hosts = rs.getExecutionInfo().getTriedHosts();
        assertThat(hosts.get(0).getAddress().getHostAddress()).isEqualTo(CCMBridge.IP_PREFIX + 1);
        assertThat(hosts.get(1).getAddress().getHostAddress()).isEqualTo(CCMBridge.IP_PREFIX + 2);
    }

    public class TryNextHostOnUnavailableRetryPolicy implements RetryPolicy{

        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return RetryDecision.tryNextHost();
        }
    }
}
