package com.ethlo.keyvalue;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Objects;

public class CasHolder<K,V,C> implements Serializable
{
	private static final long serialVersionUID = 8391662893296912918L;
	
	private C casValue;
	private K key;
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

	public CasHolder<K,V,C> setValue(V value)
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
		if (obj instanceof CasHolder)
		{
			@SuppressWarnings("unchecked")
			final CasHolder<K,V,C> b = (CasHolder<K,V,C>) obj;
			return equals(key, b.key)
				&& equals(value, b.value)
				&& equals(casValue, b.casValue);
		}
		return false;
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
			if (! Objects.equals(Array.get(a, i), Array.get(b, i)))
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
}