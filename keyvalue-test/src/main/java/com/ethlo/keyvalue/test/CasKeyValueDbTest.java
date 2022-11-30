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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;

public abstract class CasKeyValueDbTest
{
    protected CasKeyValueDb<ByteArrayKey, byte[], Long> casKeyValueDb;

    protected abstract CasKeyValueDb<ByteArrayKey, byte[], Long> getCasKeyValueDb();

    @BeforeEach
    void before()
    {
        casKeyValueDb = Objects.requireNonNull(getCasKeyValueDb());
        casKeyValueDb.clear();
    }

    @AfterEach
    void after()
    {
        casKeyValueDb.close();
    }

    @Test
    void testGetAll()
    {
        final ByteArrayKey keyBytes0 = new ByteArrayKey(new byte[]{0, 0});
        final ByteArrayKey keyBytes1 = new ByteArrayKey(new byte[]{1, 0});
        final ByteArrayKey keyBytes2 = new ByteArrayKey(new byte[]{1, 1});
        final ByteArrayKey keyBytes3 = new ByteArrayKey(new byte[]{1, 2});
        final ByteArrayKey keyBytes4 = new ByteArrayKey(new byte[]{2, 0});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);

        casKeyValueDb.put(keyBytes0, valueBytes);
        casKeyValueDb.put(keyBytes1, valueBytes);
        casKeyValueDb.put(keyBytes2, valueBytes);
        casKeyValueDb.put(keyBytes3, valueBytes);
        casKeyValueDb.put(keyBytes4, valueBytes);

        final Set<ByteArrayKey> keys = new TreeSet<>();
        keys.add(keyBytes0);
        keys.add(keyBytes1);
        keys.add(keyBytes2);

        final Map<ByteArrayKey, byte[]> result = casKeyValueDb.getAll(keys);
        assertThat(result).hasSize(3);
        assertThat(result.keySet()).containsExactlyInAnyOrder(keyBytes0, keyBytes1, keyBytes2);
    }

    @Test
    void putAndGetCompare()
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        casKeyValueDb.putCas(new CasHolder<>(null, keyBytes, valueBytes));
        final byte[] retVal = casKeyValueDb.get(keyBytes);
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
        casKeyValueDb.putAll(map);
        assertThat(casKeyValueDb.get(keyBytes1)).isEqualTo(valueBytes1);
        assertThat(casKeyValueDb.get(keyBytes2)).isEqualTo(valueBytes2);
        assertThat(casKeyValueDb.get(keyBytes3)).isEqualTo(valueBytes3);
    }

    @Test
    void testCas()
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{4, 5, 6, 7, 9, 9});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        casKeyValueDb.put(keyBytes, valueBytes);

        final CasHolder<ByteArrayKey, byte[], Long> res = casKeyValueDb.getCas(keyBytes);
        assertThat(res.getKey()).isEqualTo(keyBytes);
        assertThat(res.getCasValue()).isEqualTo(0);
        assertThat(res.getValue()).isEqualTo(valueBytes);

        final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsMakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
        res.setValue(valueBytesUpdated);
        casKeyValueDb.putCas(res);
        final CasHolder<ByteArrayKey, byte[], Long> cas = casKeyValueDb.getCas(res.getKey());
        assertThat(cas.getValue()).isEqualTo(valueBytesUpdated);
        assertThat(cas.getCasValue()).isEqualTo(1);
    }
}
