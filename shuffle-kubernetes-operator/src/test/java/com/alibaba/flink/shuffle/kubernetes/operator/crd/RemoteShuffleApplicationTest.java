/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.kubernetes.operator.crd;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@link RemoteShuffleApplication}. */
public class RemoteShuffleApplicationTest {

    private final RemoteShuffleApplication shuffleApp = new RemoteShuffleApplication();

    @Test
    public void testVersion() {
        assertEquals(shuffleApp.getVersion(), "v1");
    }

    @Test
    public void testGroup() {
        assertEquals(shuffleApp.getGroup(), "shuffleoperator.alibaba.com");
    }

    @Test
    public void testSingular() {
        assertEquals(shuffleApp.getSingular(), "remoteshuffle");
    }

    @Test
    public void testPlural() {
        assertEquals(shuffleApp.getPlural(), "remoteshuffles");
    }

    @Test
    public void testCRDName() {
        assertEquals(shuffleApp.getCRDName(), "remoteshuffles.shuffleoperator.alibaba.com");
    }

    @Test
    public void testScope() {
        assertEquals(shuffleApp.getScope(), "Namespaced");
    }

    @Test
    public void testKind() {
        assertEquals(shuffleApp.getKind(), "RemoteShuffle");
    }
}
