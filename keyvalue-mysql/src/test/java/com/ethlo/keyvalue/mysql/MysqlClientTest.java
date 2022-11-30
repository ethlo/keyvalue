package com.ethlo.keyvalue.mysql;

/*-
 * #%L
 * Key/value MySQL implementation
 * %%
 * Copyright (C) 2013 - 2020 Morten Haraldsen (ethlo)
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;
import org.springframework.data.util.CloseableIterator;

import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.keys.ByteArrayKey;

class MysqlClientTest extends AbstractTest
{
    final ByteArrayKey keyBytes0 = new ByteArrayKey(new byte[]{0, 0});
    final ByteArrayKey keyBytes1 = new ByteArrayKey(new byte[]{1, 0});
    final ByteArrayKey keyBytes2 = new ByteArrayKey(new byte[]{1, 1});
    final ByteArrayKey keyBytes3 = new ByteArrayKey(new byte[]{1, 2});
    final ByteArrayKey keyBytes4 = new ByteArrayKey(new byte[]{2, 0});

    @Test
    void testGetAll()
    {
        final Map<ByteArrayKey, byte[]> data = createFiveEntries();
        db.putAll(data);

        final Set<ByteArrayKey> keys = new TreeSet<>();
        keys.add(keyBytes0);
        keys.add(keyBytes1);
        keys.add(keyBytes2);

        final Map<ByteArrayKey, byte[]> result = db.getAll(keys);
        assertThat(result).hasSize(3);
        assertThat(result.keySet()).containsExactlyInAnyOrder(keyBytes0, keyBytes1, keyBytes2);
    }

    @Test
    void putAndGetCompare()
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        db.putCas(new CasHolder<>(null, keyBytes, valueBytes));
        final byte[] retVal = db.get(keyBytes);
        assertThat(retVal).isEqualTo(valueBytes);
    }

    @Test
    void putAll()
    {
        final ByteArrayKey keyBytes1 = new ByteArrayKey(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        final ByteArrayKey keyBytes2 = new ByteArrayKey(new byte[]{0, 1, 2, 3, 4, 5, 6, 8});
        final ByteArrayKey keyBytes3 = new ByteArrayKey(new byte[]{0, 1, 2, 3, 4, 5, 6, 9});
        final byte[] valueBytes1 = "ThisIsTheDataToStoreSoLetsMakeItABitLonger1".getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytes2 = "ThisIsTheDataToStoreSoLetsMakeItABitLonger2".getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytes3 = "ThisIsTheDataToStoreSoLetsMakeItABitLonger3".getBytes(StandardCharsets.UTF_8);
        final Map<ByteArrayKey, byte[]> map = new LinkedHashMap<>();
        map.put(keyBytes1, valueBytes1);
        map.put(keyBytes2, valueBytes2);
        map.put(keyBytes3, valueBytes3);
        db.putAll(map);
        assertThat(db.get(keyBytes1)).isEqualTo(valueBytes1);
        assertThat(db.get(keyBytes2)).isEqualTo(valueBytes2);
        assertThat(db.get(keyBytes3)).isEqualTo(valueBytes3);
    }

    @Test
    void testCas()
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{4, 5, 6, 7, 9, 9});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        db.put(keyBytes, valueBytes);

        final CasHolder<ByteArrayKey, byte[], Long> res = db.getCas(keyBytes);
        assertThat(res.getKey()).isEqualTo(keyBytes);
        assertThat(res.getCasValue()).isEqualTo(0);
        assertThat(res.getValue()).isEqualTo(valueBytes);

        final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsMakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
        res.setValue(valueBytesUpdated);
        db.putCas(res);
        final CasHolder<ByteArrayKey, byte[], Long> cas = db.getCas(res.getKey());
        assertThat(cas.getValue()).isEqualTo(valueBytesUpdated);
        assertThat(cas.getCasValue()).isEqualTo(1);
    }

    @Test
    void testIterate()
    {
        final Map<ByteArrayKey, byte[]> data = createFiveEntries();
        db.putAll(data);

        final ByteArrayKey prefixKey = new ByteArrayKey(new byte[]{1});
        try (final CloseableIterator<Map.Entry<ByteArrayKey, byte[]>> iter = db.iteratorFromPrefix(prefixKey))
        {
            assertThat(StreamSupport.stream(iter.spliterator(), false).toList()).hasSize(3);
        }
    }

    private Map<ByteArrayKey, byte[]> createFiveEntries()
    {
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        final Map<ByteArrayKey, byte[]> data = new TreeMap<>();
        data.put(keyBytes0, valueBytes);
        data.put(keyBytes1, valueBytes);
        data.put(keyBytes2, valueBytes);
        data.put(keyBytes3, valueBytes);
        data.put(keyBytes4, valueBytes);
        return data;
    }

    @Test
    void testMutate()
    {
        final ByteArrayKey key = new ByteArrayKey(new byte[]{6});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsMakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);

        db.put(key, valueBytes);

        db.mutate(key, input -> valueBytesUpdated);

        final CasHolder<ByteArrayKey, byte[], Long> res = db.getCas(key);
        assertThat(res.getCasValue()).isEqualTo(1);
        assertThat(valueBytesUpdated).isEqualTo(res.getValue());
    }
}
