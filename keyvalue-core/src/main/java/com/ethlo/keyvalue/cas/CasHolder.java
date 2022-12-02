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

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Objects;

public record CasHolder<K extends Comparable<K>, V, C extends Comparable<C>>(C version, K key,
                                                                             V value) implements Serializable, Comparable<CasHolder<K, V, C>>
{
    @Override
    public int hashCode()
    {
        return Objects.hash(key, value, version);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null || obj.getClass() != getClass())
        {
            return false;
        }

        @SuppressWarnings("unchecked") final CasHolder<K, V, C> b = (CasHolder<K, V, C>) obj;
        return equals(key, b.key)
                && equals(value, b.value)
                && equals(version, b.version);
    }

    @Override
    public String toString()
    {
        return "CasHolder [cas=" + version + ", key=" + key + ", value=" + valueToString(value) + "]";
    }

    private String valueToString(V value)
    {
        if (value == null)
        {
            return "null";
        }

        if (value.getClass().isArray())
        {
            final int len = Array.getLength(value);
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++)
            {
                final Object entry = Array.get(value, i);
                sb.append(entry);
                if (i < len - 1)
                {
                    sb.append(",");
                }
            }
            return sb.toString();
        }

        return value.toString();
    }

    private boolean equals(Object a, Object b)
    {
        if (a == null && b == null)
        {
            return true;
        }

        if (a == null)
        {
            return false;
        }

        if (b == null)
        {
            return false;
        }

        if (a.getClass().isArray())
        {
            return arrayEquals(a, b);
        }
        else
        {
            return Objects.equals(a, b);
        }
    }

    private boolean arrayEquals(Object a, Object b)
    {
        final int aLen = Array.getLength(a);
        final int bLen = Array.getLength(b);

        if (aLen != bLen)
        {
            return false;
        }

        for (int i = 0; i < aLen; i++)
        {
            if (!Objects.equals(Array.get(a, i), Array.get(b, i)))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compareTo(final CasHolder<K, V, C> casHolder)
    {
        return key.compareTo(casHolder.key);
    }

    public CasHolder<K, V, C> ofValue(V value)
    {
        return new CasHolder<>(version, key, value);
    }
}
