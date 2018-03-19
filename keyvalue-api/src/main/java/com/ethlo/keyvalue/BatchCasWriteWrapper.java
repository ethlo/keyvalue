package com.ethlo.keyvalue;

/*-
 * #%L
 * Key/Value API
 * %%
 * Copyright (C) 2015 - 2018 Morten Haraldsen (ethlo)
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

import com.ethlo.keyvalue.keys.Key;

/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 * @param <C>
 */
public class BatchCasWriteWrapper<K extends Key, V, C> implements BatchCasKeyValueDb<K, V, C>
{
	private final List<CasHolder<K, V, C>> buffer = new LinkedList<>();
	private final BatchCasKeyValueDb<K,V,C> kvdb;
	private int batchSize;;
		
	public BatchCasWriteWrapper(BatchCasKeyValueDb<K,V,C> kvdb, int batchSize)
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
		kvdb.putBatch(casList);
	}

	public void put(K key, V value)
	{
		kvdb.put(key, value);
	}

	public void delete(K key)
	{
		kvdb.delete(key);
	}

	public void clear()
	{
		kvdb.clear();
	}

	public void close()
	{
		kvdb.close();
	}

	public CasHolder<K, V, C> getCas(K key)
	{
		return kvdb.getCas(key);
	}

	public void flush()
	{
		this.kvdb.putBatch(buffer);
		buffer.clear();
	}
}
