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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ethlo.keyvalue.cas.BatchCasKeyValueDb;
import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.keys.Key;

/**
 * @param <K>
 * @param <V>
 * @param <C>
 * @author Morten Haraldsen
 */
public class BatchCasWriteWrapper<K extends Key<K>, V, C extends Comparable<C>> implements BatchCasKeyValueDb<K, V, C>
{
    private final List<CasHolder<K, V, C>> buffer = new LinkedList<>();
    private final BatchCasKeyValueDb<K, V, C> kvdb;
    private final int batchSize;

    public BatchCasWriteWrapper(BatchCasKeyValueDb<K, V, C> kvdb, int batchSize)
    {
        this.kvdb = kvdb;
        this.batchSize = batchSize;
    }

    public void putCas(CasHolder<K, V, C> casHolder)
    {
        this.buffer.add(casHolder);
        if (buffer.size() >= batchSize)
        {
            flush();
        }
    }

    public V get(K key)
    {
        return kvdb.get(key);
    }

    public void putBatch(List<CasHolder<K, V, C>> casList)
    {
        kvdb.putAll(casList);
    }

    @Override
    public void put(K key, V value)
    {
        kvdb.put(key, value);
    }

    @Override
    public void putAll(final Map<K, V> values)
    {
        this.kvdb.putAll(values);
    }

    @Override
    public void delete(K key)
    {
        kvdb.delete(key);
    }

    @Override
    public void clear()
    {
        kvdb.clear();
    }

    @Override
    public void close()
    {
        kvdb.close();
    }

    @Override
    public CasHolder<K, V, C> getCas(K key)
    {
        return kvdb.getCas(key);
    }

    public void flush()
    {
        this.kvdb.putAll(buffer);
        buffer.clear();
    }

    @Override
    public void putAll(final List<CasHolder<K, V, C>> casList)
    {
        kvdb.putAll(casList);
    }
}
