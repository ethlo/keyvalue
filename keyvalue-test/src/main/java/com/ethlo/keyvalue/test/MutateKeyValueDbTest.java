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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ethlo.keyvalue.MutableKeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;

public abstract class MutateKeyValueDbTest
{
    protected MutableKeyValueDb<ByteArrayKey, byte[]> mutableKeyValueDb;

    protected abstract MutableKeyValueDb<ByteArrayKey, byte[]> getMutableKeyValueDb();

    @BeforeEach
    void before()
    {
        mutableKeyValueDb = getMutableKeyValueDb();
    }

    @AfterEach
    void after()
    {
        mutableKeyValueDb.close();
    }

    @Test
    void testMutate()
    {
        final ByteArrayKey key = new ByteArrayKey(new byte[]{6});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsMakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);

        mutableKeyValueDb.put(key, valueBytes);

        mutableKeyValueDb.mutate(key, input -> valueBytesUpdated);

        final byte[] res = mutableKeyValueDb.get(key);
        assertThat(valueBytesUpdated).isEqualTo(res);
    }
}
