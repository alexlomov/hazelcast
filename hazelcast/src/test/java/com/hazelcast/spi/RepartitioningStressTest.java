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

package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests how well hazelcast is able to deal with a cluster where members are leaving and joining all the time.
 * So when partitions are moving around, is Hazelcast able to do readonly operations, and is it able to do update
 * operations with backups? That is what this test is all about.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class RepartitioningStressTest extends HazelcastTestSupport {

    private BlockingQueue<HazelcastInstance> queue = new LinkedBlockingQueue<HazelcastInstance>();
    private HazelcastInstance hz;
    private TestHazelcastInstanceFactory instanceFactory;

    private final static long DURATION_SECONDS = 300;
    private final static int THREAD_COUNT = 16;

    @After
    public void killAllHazelcastInstances() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        killAllHazelcastInstances();
        instanceFactory = this.createHazelcastInstanceFactory(100000);
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_HEALTH_MONITORING_LEVEL, "OFF");
        hz = instanceFactory.newHazelcastInstance(config);

        for (int k = 0; k < 5; k++) {
            queue.add(instanceFactory.newHazelcastInstance(config));
        }
    }

    @Test
    public void callWithBackups() throws InterruptedException {
        final IMap<Integer, Integer> map = hz.getMap("map");
        final int itemCount = 10000;
        for (int k = 0; k < itemCount; k++) {
            map.put(k, 0);
        }

        RestartThread restartThread = new RestartThread();
        restartThread.start();

        UpdateThread[] testThreads = new UpdateThread[THREAD_COUNT];
        for (int l = 0; l < testThreads.length; l++) {
            testThreads[l] = new UpdateThread(l, itemCount, map);
            testThreads[l].start();
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(DURATION_SECONDS));
        restartThread.stop = true;
        for (TestThread t : testThreads) {
            t.join(TimeUnit.MINUTES.toMillis(1));
            t.assertDiedPeacefully();
        }

        //now we are going to verify that the values in the map have incremented the expected amount
        //and that no backups have gone lost.
        int[] expectedCounts = new int[itemCount];
        for (UpdateThread updateThread : testThreads) {
            for (int k = 0; k < expectedCounts.length; k++) {
                expectedCounts[k] += updateThread.updates[k];
            }
        }

        for (int k = 0; k < expectedCounts.length; k++) {
            int expected = expectedCounts[k];
            int actual = map.get(k);
            assertEquals("value is different for key:"+k,expected, actual);
        }
    }

    @Test
    public void callWithoutBackups() throws InterruptedException {
        final IMap<Integer, Integer> map = hz.getMap("map");
        final int itemCount = 10000;
        for (int k = 0; k < itemCount; k++) {
            map.put(k, k);
        }

        RestartThread restartThread = new RestartThread();
        restartThread.start();

        TestThread[] testThreads = new TestThread[THREAD_COUNT];
        for (int k = 0; k < testThreads.length; k++) {
            testThreads[k] = new TestThread("GetThread-" + k) {
                @Override
                void doRun() {
                    long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

                    Random random = new Random();
                    for (; ; ) {
                        int key = random.nextInt(itemCount);
                        assertEquals(new Integer(key), map.get(key));
                        if (System.currentTimeMillis() > endTime) {
                            break;
                        }
                    }
                }
            };
            testThreads[k].start();
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(DURATION_SECONDS));

        for (TestThread t : testThreads) {
            t.join(TimeUnit.MINUTES.toMillis(1));
            t.assertDiedPeacefully();
        }

        restartThread.stop = true;
    }

    private abstract class TestThread extends Thread {
        private volatile Throwable t;

        protected TestThread(String name) {
            super(name);
        }

        public final void run() {
            try {
                doRun();
            } catch (Throwable t) {
                this.t = t;
                t.printStackTrace();
            }
        }

        abstract void doRun();

        public void assertDiedPeacefully() {
            assertFalse(isAlive());

            if (t != null) {
                t.printStackTrace();
                fail(getName() + " failed with an exception:" + t.getMessage());
            }
        }
    }

    public class RestartThread extends Thread {

        private volatile boolean stop;

        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(10000);
                    HazelcastInstance hz = queue.take();
                    hz.shutdown();
                    queue.add(instanceFactory.newHazelcastInstance());
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private class UpdateThread extends TestThread {
        private final int itemCount;
        private final IMap<Integer, Integer> map;
        private final int[] updates;

        public UpdateThread(int l, int itemCount, IMap<Integer, Integer> map) {
            super("Thread-" + l);
            this.itemCount = itemCount;
            this.map = map;
            this.updates = new int[itemCount];
        }

        @Override
        void doRun() {
            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

            Random random = new Random();
            for (; ; ) {
                int key = random.nextInt(itemCount);
                int oldValue = map.get(key);
                int newValue = oldValue++;
                if (map.replace(key, oldValue, newValue)) {
                    updates[key]++;
                }

                if (System.currentTimeMillis() > endTime) {
                    break;
                }
            }
        }
    }
}
