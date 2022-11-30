package com.ethlo.keyvalue.mysql;

/*-
 * #%L
 * Key-Value - MySQL implementation
 * %%
 * Copyright (C) 2013 - 2022 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

import com.ethlo.binary.UnsignedUtil;
import com.ethlo.keyvalue.MutableKeyValueDb;
import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;
import com.ethlo.keyvalue.test.MutateKeyValueDbTest;

@Transactional
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = TestCfg.class)
class StandardMutateTest extends MutateKeyValueDbTest
{
    @Autowired
    private MysqlClientManagerImpl mysqlClientManager;

    @Test
    void mutateStressTest() throws Exception
    {
        final int threadCount = 100;
        final int iterations = 200;
        final List<Callable<Void>> threadArr = new ArrayList<>(threadCount);
        final AtomicInteger failed = new AtomicInteger();
        for (int threadIdx = 0; threadIdx < threadCount; threadIdx++)
        {
            threadArr.add(() ->
            {
                for (int j = 0; j < iterations; j++)
                {
                    final ByteArrayKey key = new ByteArrayKey(UnsignedUtil.encodeUnsignedInt(j));

                    boolean success = false;
                    while (!success)
                    {
                        try
                        {
                            mutableKeyValueDb.mutate(key, input ->
                            {
                                final long curCount = input != null ? UnsignedUtil.decodeUnsignedInt(input) : 0;

                                // Increment by 1
                                return UnsignedUtil.encodeUnsignedInt(curCount + 1);
                            });
                            success = true;
                        }
                        catch (OptimisticLockingFailureException exc)
                        {
                            failed.incrementAndGet();
                        }
                    }
                }
                return null;
            });
        }

        // Launch threads
        final ExecutorService exec = Executors.newFixedThreadPool(threadArr.size());
        final List<Future<Void>> result = exec.invokeAll(threadArr);

        // Check results
        for (Future<Void> res : result)
        {
            res.get();
        }

        // Check data is correct
        for (int i = 0; i < iterations; i++)
        {
            final ByteArrayKey key = new ByteArrayKey(UnsignedUtil.encodeUnsignedInt(i));
            final long count = UnsignedUtil.decodeUnsignedInt(mutableKeyValueDb.get(key));
            assertThat(count).isEqualTo(threadCount);
        }
        System.out.println("Failed attempts " + failed.get());
    }

    @Test
    void mutateStressTestSameKey() throws Exception
    {
        final int threadCount = 25;
        final int iterations = 100;
        final List<Callable<Void>> threadArr = new ArrayList<>(threadCount);
        final ByteArrayKey key = new ByteArrayKey(UnsignedUtil.encodeUnsignedInt((int) (Math.random() * Integer.MAX_VALUE)));
        final AtomicInteger failed = new AtomicInteger();
        for (int threadIdx = 0; threadIdx < threadCount; threadIdx++)
        {
            threadArr.add(() -> {
                for (int j = 0; j < iterations; j++)
                {
                    boolean success = false;
                    while (!success)
                    {
                        try
                        {
                            mutableKeyValueDb.mutate(key, input ->
                            {
                                final long curCount = input != null ? UnsignedUtil.decodeUnsignedInt(input) : 0;

                                // Increment by 1
                                return UnsignedUtil.encodeUnsignedInt(curCount + 1);
                            });

                            success = true;
                        }
                        catch (OptimisticLockingFailureException exc)
                        {
                            failed.incrementAndGet();
                        }
                    }
                }
                return null;
            });
        }

        // Launch threads
        final ExecutorService exec = Executors.newFixedThreadPool(threadArr.size());
        final List<Future<Void>> result = exec.invokeAll(threadArr);

        // Check results
        for (Future<Void> res : result)
        {
            res.get();
        }

        final long count = UnsignedUtil.decodeUnsignedInt(mutableKeyValueDb.get(key));
        System.out.println(count + " (failed attempts " + failed.get() + ")");
        assertThat(count).isEqualTo(threadCount * iterations);
    }

    @Override
    protected MutableKeyValueDb<ByteArrayKey, byte[]> getMutableKeyValueDb()
    {
        return mysqlClientManager.getDb("_kvtest", true, new HexKeyEncoder(), new NopDataCompressor());
    }
}
