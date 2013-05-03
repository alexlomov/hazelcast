/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.finalTest;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MigrationTest {

    final String mapName = "map";

    @BeforeClass
    @AfterClass
    public static void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMapMigration() throws InterruptedException {
        StaticNodeFactory nodeFactory = new StaticNodeFactory(3);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        int size = 1000;

        Map map = instance1.getMap("testMapMigration");
        for (int i = 0; i < size; i++) {
            map.put(i,i);
        }

        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        Thread.sleep(1000);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), i);
        }

        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        Thread.sleep(1000);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), i);
        }

    }




}