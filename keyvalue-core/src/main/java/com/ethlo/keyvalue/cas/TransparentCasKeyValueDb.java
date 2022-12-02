package com.ethlo.keyvalue.cas;

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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.keyvalue.KeyValueDb;
import com.ethlo.keyvalue.keys.Key;

/**
 * Throw-away wrapper around a single batch of writes for key-value database, allowing the CAS values to be transparently handled.
 *
 * @param <K> The key type
 * @param <V> The value type
 * @author Morten Haraldsen
 */
public class TransparentCasKeyValueDb<K extends Key<K>, V, C extends Comparable<C>> implements KeyValueDb<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(TransparentCasKeyValueDb.class);

    private final CasKeyValueDb<K, V, C> delegate;
    private final ConcurrentHashMap<K, ValueHolder<C>> revisionHolder;

    public TransparentCasKeyValueDb(CasKeyValueDb<K, V, C> delegate)
    {
        this.delegate = delegate;
        this.revisionHolder = new ConcurrentHashMap<>(16, 0.75f, 4);
    }

    @Override
    public V get(K key)
    {
        final CasHolder<K, V, C> casHolder = delegate.getCas(key);
        if (casHolder != null)
        {
            final C currentCasValue = casHolder.version();
            revisionHolder.put(key, new ValueHolder<>(currentCasValue));
            logger.debug("get({}) with CAS value {}", key, currentCasValue);
            return casHolder.value();
        }
        return null;
    }

    @Override
    public void put(K key, V value)
    {
        try
        {
            final ValueHolder<C> casValue = revisionHolder.get(key);
            if (casValue != null)
            {
                final C cas = casValue.value != null ? casValue.value : null;
                logger.debug("put({}) with CAS value {}", key, cas);
                delegate.putCas(new CasHolder<>(cas, key, value));
            }
            else
            {
                final CasHolder<K, V, C> casFromDB = delegate.getCas(key);
                if (casFromDB != null)
                {
                    delegate.putCas(new CasHolder<>(casFromDB.version(), key, value));
                }
                else
                {
                    delegate.putCas(new CasHolder<>(null, key, value));
                }
            }
        } finally
        {
            this.revisionHolder.remove(key);
        }
    }

    @Override
    public void delete(K key)
    {
        this.revisionHolder.remove(key);
        this.delegate.delete(key);
    }

    @Override
    public void clear()
    {
        this.revisionHolder.clear();
        this.delegate.clear();
    }

    @Override
    public void close()
    {
        this.delegate.close();
    }

    private record ValueHolder<T>(T value)
    {
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            else if (obj == null || obj.getClass() != ValueHolder.class)
            {
                return false;
            }
            else
            {
                return Objects.equals(this.value, ((ValueHolder<?>) obj).value);
            }
        }

        public int hashCode()
        {
            return Objects.hashCode(this.value);
        }
    }
}
