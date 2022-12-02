package com.ethlo.keyvalue.hashmap;

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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.util.CloseableIterator;

import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;

public class HashmapKeyValueDbImpl implements HashmapKeyValueDb
{
    private final ConcurrentSkipListMap<ByteArrayKey, CasHolder<ByteArrayKey, byte[], Long>> data = new ConcurrentSkipListMap<>();

    @Override
    public byte[] get(ByteArrayKey key)
    {
        return Optional.ofNullable(data.get(key)).map(CasHolder::value).orElse(null);
    }

    @Override
    public void put(ByteArrayKey key, byte[] value)
    {
        data.put(key, new CasHolder<>(0L, key, value));
    }

    @Override
    public void clear()
    {
        data.clear();
    }

    @Override
    public void close()
    {
        // Nothing to close
    }

    @Override
    public CasHolder<ByteArrayKey, byte[], Long> getCas(ByteArrayKey key)
    {
        return data.get(key);
    }

    @Override
    public void putCas(CasHolder<ByteArrayKey, byte[], Long> casHolder)
    {
        data.compute(casHolder.key(), (k, existing) ->
        {
            if (existing == null)
            {
                return new CasHolder<>(1L, k, casHolder.value());
            }
            else
            {
                if (!Objects.equals(casHolder.version(), existing.version()))
                {
                    throw new OptimisticLockingFailureException("CAS value " + casHolder.version() + " is not matching expected " + existing.version() + " for key " + k);
                }
                return new CasHolder<>(existing.version() + 1L, k, casHolder.value());
            }
        });
    }

    @Override
    public void delete(ByteArrayKey key)
    {
        this.data.remove(key);
    }

    public long getTotalSize()
    {
        long total = 0;
        for (CasHolder<ByteArrayKey, byte[], Long> e : data.values())
        {
            total += e.value().length;
        }
        return total;
    }

    @Override
    public CloseableIterator<Entry<ByteArrayKey, byte[]>> iterator()
    {
        return getCloseableIterator(null);
    }

    @Override
    public CloseableIterator<Entry<ByteArrayKey, byte[]>> iteratorFromPrefix(final ByteArrayKey keyPrefix)
    {
        return getCloseableIterator(keyPrefix);
    }

    private CloseableIterator<Entry<ByteArrayKey, byte[]>> getCloseableIterator(ByteArrayKey keyPrefix)
    {
        final Iterator<Entry<ByteArrayKey, CasHolder<ByteArrayKey, byte[], Long>>> iter = data.entrySet().iterator();

        if (keyPrefix != null)
        {
            final HexKeyEncoder enc = new HexKeyEncoder();
            final String targetKey = enc.toString(keyPrefix.getByteArray());

            while (iter.hasNext())
            {
                final Entry<ByteArrayKey, CasHolder<ByteArrayKey, byte[], Long>> e = iter.next();
                final String hexKey = enc.toString(e.getKey().getByteArray());
                if (hexKey.startsWith(targetKey))
                {
                    break;
                }
            }
        }

        return new CloseableIterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            @Override
            public Entry<ByteArrayKey, byte[]> next()
            {
                final CasHolder<ByteArrayKey, byte[], Long> d = iter.next().getValue();
                return new AbstractMap.SimpleEntry<>(d.key(), d.value());
            }


            @Override
            public void close()
            {
                // Nothing to close
            }
        };
    }

    @Override
    public byte[] mutate(final ByteArrayKey key, final UnaryOperator<byte[]> mutator)
    {
        final AtomicReference<byte[]> result = new AtomicReference<>();
        data.compute(key, (k, v) ->
        {
            byte[] res;

            if (v == null || v.version() == null)
            {
                res = mutator.apply(null);
                result.set(res);
                return new CasHolder<>(0L, key, res);
            }
            else
            {
                res = mutator.apply(v.value());
                result.set(res);
                return new CasHolder<>(v.version() + 1, key, res);
            }
        });
        return result.get();
    }
}
