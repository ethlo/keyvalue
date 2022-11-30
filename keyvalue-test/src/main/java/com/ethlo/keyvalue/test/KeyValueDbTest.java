package com.ethlo.keyvalue.test;

/*-
 * #%L
 * Key/Value API
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ethlo.keyvalue.KeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;

public abstract class KeyValueDbTest
{
    protected KeyValueDb<ByteArrayKey, byte[]> db;

    protected abstract KeyValueDb<ByteArrayKey, byte[]> getDb();

    @BeforeEach
    void before()
    {
        this.db = getDb();
    }

    @AfterEach
    void after()
    {
        this.db.close();
    }

    @Test
    void testPutAndGet()
    {
        final ByteArrayKey key = new ByteArrayKey("abcd".getBytes());
        final byte[] value = "fghijklmnopqrstuvwxyz".getBytes();
        db.put(key, value);
        final byte[] retVal = db.get(key);
        assertThat(retVal).isEqualTo(value);
    }

    @Test
    void testDelete()
    {
        final ByteArrayKey key = new ByteArrayKey("abcd".getBytes());
        final byte[] value = "fghijklmnopqrstuvwxyz".getBytes();
        db.put(key, value);
        final byte[] retVal = db.get(key);
        assertThat(retVal).isEqualTo(value);
        db.delete(key);
        assertThat(db.get(key)).isNull();
    }

    @Test
    void testClear()
    {
        final ByteArrayKey key = new ByteArrayKey("abcd".getBytes());
        final ByteArrayKey key2 = new ByteArrayKey("ef".getBytes());
        final byte[] value = "fghijklmnopqrstuvwxyz".getBytes();
        db.put(key, value);
        db.put(key2, value);
        final byte[] retVal = db.get(key);
        final byte[] retVal2 = db.get(key2);
        assertThat(retVal).isEqualTo(value);
        assertThat(retVal2).isEqualTo(value);
        db.clear();
        assertThat(db.get(key)).isNull();
        assertThat(db.get(key2)).isNull();
    }
}
