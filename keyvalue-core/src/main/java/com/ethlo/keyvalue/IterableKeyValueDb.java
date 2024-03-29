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

import org.springframework.data.util.CloseableIterator;

import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.Key;

/**
 * @author mha
 */
public interface IterableKeyValueDb<K extends Key<K>, V> extends KeyValueDb<K, V>
{
    CloseableIterator<Map.Entry<ByteArrayKey, byte[]>> iterator();

    CloseableIterator<Map.Entry<ByteArrayKey, byte[]>> iteratorFromPrefix(ByteArrayKey keyPrefix);
}
