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

import java.util.List;

import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.UnavailableException;

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

    public void beforeMethod(RetryPolicy retryPolicy) {
        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.ipOfNode(1))
            .withRetryPolicy(retryPolicy)
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
        beforeMethod(new LoggingRetryPolicy(new TryNextHostOnUnavailableRetryPolicy()));
        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withResult(PrimingRequest.Result.unavailable)
                    .build()
            );
        SimpleStatement statement = new SimpleStatement("mock query");
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        ResultSet rs = session.execute(statement);
        List<Host> hosts = rs.getExecutionInfo().getTriedHosts();
        assertThat(hosts.size()).isEqualTo(2);
        assertThat(hosts.get(0).getAddress().getHostAddress()).isEqualTo(CCMBridge.IP_PREFIX + 1);
        assertThat(hosts.get(1).getAddress().getHostAddress()).isEqualTo(CCMBridge.IP_PREFIX + 2);
    }

    @Test(groups = "short")
    public void should_try_next_host_only_once_by_default() {
        beforeMethod(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE));
        boolean unavailableException = false;
        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withResult(PrimingRequest.Result.unavailable)
                    .build()
            ).prime(2, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.unavailable)
                .build()
        );
        SimpleStatement statement = new SimpleStatement("mock query");
        try {
            session.execute(statement);
        } catch (UnavailableException e) {
            // We must get an UnavailableException because we retried
            // once on another node, and the second time, the default
            // retry policy throws an exception.
            unavailableException = true;
            assertThat(errors.getRetries().getCount()).isEqualTo(1);
        }
        assertThat(unavailableException).isTrue();
    }

    @AfterMethod(groups = "short")
    public void afterMethod() {
        scassandras.clearAllPrimes();
        if (cluster != null)
            cluster.close();
    }

    @AfterClass(groups = "short")
    public void afterClass() {
        if (scassandras != null)
            scassandras.stop();
    }

    public class TryNextHostOnUnavailableRetryPolicy implements RetryPolicy {

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
            return RetryDecision.tryNextHost(cl);
        }
    }
}
