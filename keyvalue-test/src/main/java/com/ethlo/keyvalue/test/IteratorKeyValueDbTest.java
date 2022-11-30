package com.ethlo.keyvalue.test;

/*-
 * #%L
 * keyvalue-test
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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.util.CloseableIterator;

import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;

public abstract class IteratorKeyValueDbTest
{
    protected IterableKeyValueDb<ByteArrayKey, byte[]> iterableKeyValueDb;

    protected abstract IterableKeyValueDb<ByteArrayKey, byte[]> getIterableKeyValueDb();

    @BeforeEach
    void before()
    {
        iterableKeyValueDb = getIterableKeyValueDb();
    }

    @AfterEach
    void after()
    {
        iterableKeyValueDb.close();
    }

    @Test
    void testIterate()
    {
        final ByteArrayKey keyBytes0 = new ByteArrayKey(new byte[]{0, 0});
        final ByteArrayKey keyBytes1 = new ByteArrayKey(new byte[]{1, 0});
        final ByteArrayKey keyBytes2 = new ByteArrayKey(new byte[]{1, 1});
        final ByteArrayKey keyBytes3 = new ByteArrayKey(new byte[]{1, 2});
        final ByteArrayKey keyBytes4 = new ByteArrayKey(new byte[]{2, 0});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        final Map<ByteArrayKey, byte[]> data = new TreeMap<>();
        data.put(keyBytes0, valueBytes);
        data.put(keyBytes1, valueBytes);
        data.put(keyBytes2, valueBytes);
        data.put(keyBytes3, valueBytes);
        data.put(keyBytes4, valueBytes);

        iterableKeyValueDb.putAll(data);

        final ByteArrayKey prefixKey = new ByteArrayKey(new byte[]{1});
        try (final CloseableIterator<Map.Entry<ByteArrayKey, byte[]>> iter = iterableKeyValueDb.iteratorFromPrefix(prefixKey))
        {
            assertThat(StreamSupport.stream(iter.spliterator(), false).toList()).hasSize(3);
        }
    }
}
