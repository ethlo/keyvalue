package com.ethlo.keyvalue;

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

import java.util.Map;

import com.ethlo.keyvalue.cas.BatchCasKeyValueDb;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.keys.Key;

/**
 * The minimal operations needed to be supported
 *
 * @author Morten Haraldsen
 * @see CasKeyValueDb
 * @see BatchCasKeyValueDb
 */
public interface KeyValueDb<K extends Key, V> extends AutoCloseable
{
    V get(K key);

    void put(K key, V value);

    default void putAll(Map<K, V> values)
    {
        values.forEach(this::put);
    }

    void delete(K key);

    void clear();

    void close();
}
