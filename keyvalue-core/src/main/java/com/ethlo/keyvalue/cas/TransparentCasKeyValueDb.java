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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.keyvalue.keys.Key;

/**
 * Throw-away wrapper around a single batch of writes for key-value database, allowing the CAS values to be transparently handled.
 *
 * @param <K> The key type
 * @param <V> The value type
 * @param <C> The cas value type
 * @author Morten Haraldsen
 */
public class TransparentCasKeyValueDb<K extends Key<K>, V, C extends Comparable<C>> implements CasKeyValueDb<K, V, C>
{
    private static final Logger logger = LoggerFactory.getLogger(TransparentCasKeyValueDb.class);
    private final CasKeyValueDb<K, V, C> db;
    private final ConcurrentHashMap<K, ValueHolder<C>> revisionHolder;

    public TransparentCasKeyValueDb(CasKeyValueDb<K, V, C> casKeyValueDb)
    {
        this.db = casKeyValueDb;
        this.revisionHolder = new ConcurrentHashMap<>(16, 0.75f, 4);
    }

    @Override
    public V get(K key)
    {
        final CasHolder<K, V, C> casHolder = db.getCas(key);
        if (casHolder != null)
        {
            final C currentCasValue = casHolder.getCasValue();
            revisionHolder.put(key, ValueHolder.of(currentCasValue));
            logger.debug("get({}) with CAS value {}", key, currentCasValue);
            return casHolder.getValue();
        }
        revisionHolder.put(key, ValueHolder.empty());
        logger.debug("get({}) with empty result", key);
        return null;
    }

    @Override
    public void put(K key, V value)
    {
        final ValueHolder<C> casValue = revisionHolder.get(key);
        if (casValue != null)
        {
            final C cas = casValue.orElse(null);
            logger.debug("put({}) with CAS value {}", key, cas);
            try
            {
                db.putCas(new CasHolder<>(cas, key, value));
            } finally
            {
                this.revisionHolder.remove(key);
            }
        }
    }

    @Override
    public void delete(K key)
    {
        this.db.delete(key);
    }

    @Override
    public void clear()
    {
        this.db.clear();
    }

    @Override
    public void close()
    {
        this.db.close();
    }

    @Override
    public CasHolder<K, V, C> getCas(K key)
    {
        return db.getCas(key);
    }

    @Override
    public void putCas(CasHolder<K, V, C> cas)
    {
        db.putCas(cas);
    }

    private static class ValueHolder<T>
    {
        private final T value;

        private ValueHolder()
        {
            this.value = null;
        }

        private ValueHolder(T value)
        {
            this.value = Objects.requireNonNull(value);
        }

        public static <T> ValueHolder<T> empty()
        {
            return new ValueHolder<>();
        }

        public static <T> ValueHolder<T> of(T value)
        {
            return new ValueHolder<>(value);
        }

        public static <T> ValueHolder<T> ofNullable(T value)
        {
            return value == null ? empty() : of(value);
        }

        public T get()
        {
            if (this.value == null)
            {
                throw new NoSuchElementException("No value present");
            }
            else
            {
                return this.value;
            }
        }

        public boolean isPresent()
        {
            return this.value != null;
        }

        public boolean isEmpty()
        {
            return this.value == null;
        }

        public void ifPresent(Consumer<? super T> action)
        {
            if (this.value != null)
            {
                action.accept(this.value);
            }

        }

        public T orElse(T other)
        {
            return this.value != null ? this.value : other;
        }

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

        public String toString()
        {
            return this.value != null ? String.format("Optional[%s]", this.value) : "Optional.empty";
        }
    }
}
