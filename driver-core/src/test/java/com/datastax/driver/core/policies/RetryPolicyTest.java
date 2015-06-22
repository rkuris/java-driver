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

import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.*;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import static com.datastax.driver.core.Assertions.assertThat;

public class RetryPolicyTest extends AbstractPoliciesTest {

    SCassandraCluster scassandras;

    Cluster cluster = null;
    Metrics.Errors errors;
    Host host1, host2, host3;
    Session session;

    private static final Object[][] DEFAULTRETRYPROVIDER = { new Object[]{ DefaultRetryPolicy.INSTANCE } };
    private static final Object[][] DEFAULTRETRYLOGGINGPROVIDER = { new Object[]{ new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE) } };

    private static final Object[][] FALLTHROUGHRETRYPROVIDER = { new Object[]{ FallthroughRetryPolicy.INSTANCE } };
    private static final Object[][] FALLTHROUGHLOGGINGRETRYPROVIDER = { new Object[]{ new LoggingRetryPolicy(FallthroughRetryPolicy.INSTANCE) } };

    private static final Object[][] ALWAYSIGNORELOGGINGRETRYPROVIDER = { new Object[]{ new LoggingRetryPolicy(AlwaysIgnoreRetryPolicy.INSTANCE) } };

    private static final Object[][] ALWAYSRETRYLOGGINGRETRYPROVIDER = { new Object[]{ new LoggingRetryPolicy(AlwaysRetryRetryPolicy.INSTANCE) } };

    @BeforeClass(groups = "short")
    public void beforeClass() {
        scassandras = new SCassandraCluster(CCMBridge.IP_PREFIX, 3);
    }

    @BeforeMethod(groups = "short")
    public void beforeMethod(Object[] retryPolicy) {
        RetryPolicy policy = (RetryPolicy)retryPolicy[0];
        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.ipOfNode(1))
            .withRetryPolicy(policy)
            .withLoadBalancingPolicy(new SortingLoadBalancingPolicy())
            .build();

        session = cluster.connect();

        host1 = TestUtils.findHost(cluster, 1);
        host2 = TestUtils.findHost(cluster, 2);
        host3 = TestUtils.findHost(cluster, 3);

        errors = cluster.getMetrics().getErrorMetrics();
    }

    /**
     * Test the DefaultRetryPolicy.
     */
    @Test(groups = "short", dataProvider = "defaultRetryPolicyProvider")
    public void should_use_default_retry_policy(Object retryPolicy) throws Throwable {
        defaultPolicyTest();
    }

    /**
     * Test the DefaultRetryPolicy with Logging enabled.
     */
    @Test(groups = "short", dataProvider = "defaultRetryLoggingPolicyProvider")
    public void should_use_default_logging_retry_policy(Object retryPolicy) throws Throwable {
        defaultPolicyTest();
    }

    /**
     * Test the FallthroughRetryPolicy.
     */
    @Test(groups = "short", dataProvider = "fallthroughRetryPolicyProvider")
    public void should_use_fallthrough_retry_policy(Object retryPolicy) throws Throwable {
        fallthroughRetryPolicy();
    }

    /**
     * Test the FallthroughRetryPolicy with Logging enabled.
     */
    @Test(groups = "short", dataProvider = "fallthroughLoggingRetryPolicyProvider")
    public void should_use_fallthrough_logging_retry_policy(Object retryPolicy) throws Throwable {
        fallthroughRetryPolicy();
    }

    /**
     * Test the AlwaysIgnoreRetryPolicy with Logging enabled.
     */
    @Test(groups = "short", dataProvider = "alwaysIgnoreRetryPolicyProvider")
    public void should_use_always_ignore_retry_policy(Object retryPolicy) throws Throwable {
        alwaysIgnoreRetryPolicy();
    }

    /**
     * Test the AlwaysRetryRetryPolicy with Logging enabled.
     */
    @Test(groups = "short", dataProvider = "alwaysRetryRetryPolicyProvider")
    public void should_use_always_retry_retry_policy(Object retryPolicy) {
        alwaysRetryRetryPolicy();
    }

    public void defaultPolicyTest() {
        /*
         * Default retry policy :
         * - ReadTimeout : rethrow
         * - WriteTimeout : rethrow
         * - Unavailable : retry once then rethrow
         */

        boolean readTimeoutException = false;
        boolean writeTimeoutException = false;
        boolean unavailableException = false;

        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build());
        try {
            mockQuery(session, 1);
        } catch (ReadTimeoutException rte) {
            assertThat(errors.getRetries().getCount()).isEqualTo(0);
            readTimeoutException = true;
        }

        assertThat(readTimeoutException).isTrue();

        scassandras.clearAllPrimes();
        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.write_request_timeout)
                .build());
        try {
            mockQuery(session, 1);
        } catch (WriteTimeoutException wte) {
            assertThat(errors.getRetries().getCount()).isEqualTo(0);
            writeTimeoutException = true;
        }

        assertThat(writeTimeoutException).isTrue();

        scassandras.clearAllPrimes();
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
        try {
            mockQuery(session, 1);
        } catch (UnavailableException e) {
            // We must get an UnavailableException because we retried
            // once on another node, and the second time, the default
            // retry policy throws an exception.
            unavailableException = true;
            assertThat(errors.getRetries().getCount()).isEqualTo(1);
        }
        assertThat(unavailableException).isTrue();
    }

    public void fallthroughRetryPolicy() {
        /*
         * Fallthrough retry policy :
         * - ReadTimeout : rethrow
         * - WriteTimeout : rethrow
         * - Unavailable : rethrow
         */

        boolean readTimeoutException = false;
        boolean writeTimeoutException = false;
        boolean unavailableException = false;

        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build());
        try {
            mockQuery(session, 1);
        } catch (ReadTimeoutException rte) {
            assertThat(errors.getRetries().getCount()).isEqualTo(0);
            readTimeoutException = true;
        }

        assertThat(readTimeoutException).isTrue();

        scassandras.clearAllPrimes();
        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.write_request_timeout)
                .build());
        try {
            mockQuery(session, 1);
        } catch (WriteTimeoutException wte) {
            assertThat(errors.getRetries().getCount()).isEqualTo(0);
            writeTimeoutException = true;
        }

        assertThat(writeTimeoutException).isTrue();

        scassandras.clearAllPrimes();
        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withResult(PrimingRequest.Result.unavailable)
                    .build()
            );
        try {
            mockQuery(session, 1);
        } catch (UnavailableException e) {
            unavailableException = true;
            assertThat(errors.getRetries().getCount()).isEqualTo(0);
        }
        assertThat(unavailableException).isTrue();
    }

    public void alwaysIgnoreRetryPolicy() {
        /*
         * Always ignore retry policy :
         * - ReadTimeout : ignore
         * - WriteTimeout : ignore
         * - Unavailable : ignore
         */

        boolean readTimeoutException = false;
        boolean writeTimeoutException = false;
        boolean unavailableException = false;

        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build());
        try {
            mockQuery(session, 1);
        } catch (Throwable e) {
            readTimeoutException = true;
        }
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(readTimeoutException).isFalse();

        scassandras.clearAllPrimes();
        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.write_request_timeout)
                .build());
        try {
            mockQuery(session, 1);
        } catch (Throwable e) {
            writeTimeoutException = true;
        }
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(writeTimeoutException).isFalse();

        scassandras.clearAllPrimes();
        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.unavailable)
                .build());
        try {
            mockQuery(session, 1);
        } catch (Throwable e) {
            unavailableException = true;
        }
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(unavailableException).isFalse();
    }

    public void alwaysRetryRetryPolicy() {
        /*
         * Always retry retry policy :
         * - ReadTimeout : retry (same cl)
         * - WriteTimeout : retry (same cl)
         * - Unavailable : retry (same cl)
         */

        resetCoordinators();
        boolean readTimeoutException = false;
        boolean writeTimeoutException = false;
        boolean unavailableException = false;

        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build());
        try {
            Thread t1 = new Thread(new QueryRunnable(session));
            t1.start();
            t1.join(10000);
            if (t1.isAlive())
                t1.interrupt();
        } catch (Throwable e) {
            readTimeoutException = true;
        }
        // This is not 100% deterministic, but let's say that it has a huge chance of success
        assertThat(errors.getRetries().getCount()).isGreaterThan(1);
        assertThat(readTimeoutException).isFalse();

        scassandras.clearAllPrimes();

        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.write_request_timeout)
                .build());
        try {
            Thread t1 = new Thread(new QueryRunnable(session));
            t1.start();
            t1.join(10000);
            if (t1.isAlive())
                t1.interrupt();
        } catch (Throwable e) {
            writeTimeoutException = true;
        }
        // This is not 100% deterministic, but let's say that it has a huge chance of success
        assertThat(errors.getRetries().getCount()).isGreaterThan(1);
        assertThat(writeTimeoutException).isFalse();

        scassandras.clearAllPrimes();

        scassandras
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(PrimingRequest.Result.unavailable)
                .build());
        try {
            Thread t1 = new Thread(new QueryRunnable(session));
            t1.start();
            t1.join(10000);
            if (t1.isAlive())
                t1.interrupt();
        } catch (Throwable e) {
            unavailableException = true;
        }
        // This is not 100% deterministic, but let's say that it has a huge chance of success
        assertThat(errors.getRetries().getCount()).isGreaterThan(1);
        assertThat(unavailableException).isFalse();
    }

    @DataProvider
    public Object[][] defaultRetryPolicyProvider() {
        return DEFAULTRETRYPROVIDER;
    }

    @DataProvider
    public Object[][] defaultRetryLoggingPolicyProvider() {
        return DEFAULTRETRYLOGGINGPROVIDER;
    }

    @DataProvider
    public Object[][] fallthroughRetryPolicyProvider() {
        return FALLTHROUGHRETRYPROVIDER;
    }

    @DataProvider
    public Object[][] fallthroughLoggingRetryPolicyProvider() {
        return FALLTHROUGHLOGGINGRETRYPROVIDER;
    }

    @DataProvider
    public Object[][] alwaysIgnoreRetryPolicyProvider() {
        return ALWAYSIGNORELOGGINGRETRYPROVIDER;
    }

    @DataProvider
    public Object[][] alwaysRetryRetryPolicyProvider() {
        return ALWAYSRETRYLOGGINGRETRYPROVIDER;
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

    private class QueryRunnable implements Runnable {
        private Session s;

        public QueryRunnable(Session s) {
            this.s = s;
        }

        public void run() {
            mockQuery(s, 1);
        }
    }
}
