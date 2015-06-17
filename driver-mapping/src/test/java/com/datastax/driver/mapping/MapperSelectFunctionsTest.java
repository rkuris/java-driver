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
package com.datastax.driver.mapping;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Lists;
import org.omg.CORBA.DynAnyPackage.Invalid;
import org.testng.annotations.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.mapping.annotations.*;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * Tests to ensure validity of {@code computed} option in
 * {@link com.datastax.driver.mapping.annotations.Column}
 */
@SuppressWarnings("unused")
public class MapperSelectFunctionsTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE user (key int primary key, v text)",
            "CREATE TYPE address(city text, street text)",
            "CREATE TABLE userad(key int primary key, v text, ads set<frozen<address>>)");
    }

    @Test(groups = "short")
    void should_add_aliases_for_fields_in_select_queries() {
        Mapper<User4> mapper = new MappingManager(session).mapper(User4.class);
        BoundStatement bs = (BoundStatement)mapper.getQuery(42);
        assertThat(bs.preparedStatement().getQueryString()).contains("SELECT \"key\" AS col1,\"v\" AS col2,writetime(\"v\") AS col3,\"ads\" AS col4");
    }


    @Test(groups = "short")
    void should_fail_if_computed_field_is_not_right_type() {
        boolean getObjectFailed = false;
        try {
            Mapper<User3> mapper = new MappingManager(session).mapper(User3.class);
        } catch (IllegalArgumentException e) {
            getObjectFailed = true;
        }
        assertThat(getObjectFailed).isTrue();
    }

    @Test(groups = "short")
    void should_fail_if_field_not_existing_and_not_marked_computed() {
        boolean getObjectFailed = false;
        boolean setObjectFailed = false;
        Mapper<User2> mapper = new MappingManager(session).mapper(User2.class);
        try {
            mapper.save(new User2(42, "helloworld"));
        } catch (InvalidQueryException e) {
            setObjectFailed = true;
        }
        assertThat(setObjectFailed).isTrue();
        try {
            User2 saved = mapper.get(42);
        } catch (InvalidQueryException e) {
            getObjectFailed = true;
        }
        assertThat(getObjectFailed).isTrue();
    }

    @Test(groups = "short")
    void should_fetch_computed_fields() {
        Mapper<User> mapper = new MappingManager(session).mapper(User.class);
        mapper.save(new User(42, "helloworld"));
        User saved = mapper.get(42);
        assertThat(saved.getWriteTime()).isNotNull();
        System.out.println("saved.getWriteTime() = " + saved.getWriteTime());
        BoundStatement bs = (BoundStatement)mapper.getQuery(42);
        System.out.println("mapper.getQuery(42) = " + bs.preparedStatement().getQueryString());
        assertThat(saved.getWriteTime()).isNotEqualTo(0);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private int key;
        private String v;

        // whitespaces in the column name inserted on purpose
        // to test the newAlias generation mechanism
        @Computed(formula = "writetime(\"v\")")
        long writeTime;

        public User() {
        }

        public User(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }

        public long getWriteTime() {
            return this.writeTime;
        }

        public void setWriteTime(long pk) {
            this.writeTime = pk;
        }
    }

    @Table(name = "user")
    public static class User2 {
        @PartitionKey
        private int key;
        private String v;

        @Column(name = "writetime(v)")
        long writeTime;

        public User2() {
        }

        public User2(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }

        public long getWriteTime() {
            return this.writeTime;
        }

        public void setWriteTime(long pk) {
            this.writeTime = pk;
        }
    }

    @Table(name = "user")
    public static class User3 {
        @PartitionKey
        private int key;
        private String v;

        @Column(name = "writetime(v)")
        byte writeTime;

        public User3() {
        }

        public User3(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }

        public byte getWriteTime() {
            return this.writeTime;
        }

        public void setWriteTime(byte pk) {
            this.writeTime = pk;
        }
    }

    @Table(name = "userad")
    public static class User4 {
        @PartitionKey
        private int key;
        private String v;

        // quotes in the column name inserted on purpose
        // to test the alias generation mechanism
        @Computed(formula = "writetime(\"v\")")
        long writeTime;

        @FrozenValue
        Set<Address> ads;

        public User4() {
        }

        public User4(int k, String val, Address address) {
            this.key = k;
            this.v = val;
            this.ads = new HashSet<Address>();
            this.ads.add(address);
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }

        public long getWriteTime() {
            return this.writeTime;
        }

        public void setWriteTime(long pk) {
            this.writeTime = pk;
        }

        public Set<Address> getAds(){
            return this.ads;
        }

        public void setAds(Set<Address> ad){
            this.ads = ad;
        }
    }


    @UDT(name = "address")
    public static class Address {

        private String street;

        @Field // not strictly required, but we want to check that the annotation works without a name
        private String city;

        public Address() {
        }

        public Address(String street, String city) {
            this.street = street;
            this.city = city;
        }

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }

}
