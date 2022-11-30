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

public class CasHolder<K, V, C extends Comparable<C>> implements Serializable, Comparable<CasHolder<K, V, C>>
{
    private final K key;
    private C casValue;
    private V value;

    public CasHolder(C casValue, K key, V value)
    {
        this.casValue = casValue;
        this.key = key;
        this.value = value;
    }

    public C getCasValue()
    {
        return casValue;
    }

    public K getKey()
    {
        return key;
    }

    public V getValue()
    {
        return value;
    }

    public CasHolder<K, V, C> setValue(V value)
    {
        this.value = value;
        return this;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((casValue == null) ? 0 : casValue.hashCode());
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
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
                && equals(casValue, b.casValue);
    }

    @Override
    public String toString()
    {
        return "CasHolder [cas=" + casValue + ", key=" + key + ", value=" + value + "]";
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

    public void setCas(C casValue)
    {
        this.casValue = casValue;
    }

    @Override
    public int compareTo(final CasHolder<K, V, C> casHolder)
    {
        if (Objects.equals(casValue, casHolder.getValue()))
        {
            return 0;
        }
        else if (casValue == null)
        {
            return -1;
        }
        return casValue.compareTo(casHolder.getCasValue());
    }
}
