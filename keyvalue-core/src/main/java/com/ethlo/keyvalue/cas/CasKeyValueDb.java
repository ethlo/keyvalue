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

import com.ethlo.keyvalue.KeyValueDb;
import com.ethlo.keyvalue.keys.Key;

/**
 * Extension of {@link KeyValueDb} that allows optimistic locking using CAS (compare-and-swap/check-and-set).
 * <p>
 * This can be used to prevent clients from updating values in the database that may have changed since the
 * client obtained the value. Methods for storing and updating information support a CAS method that allows
 * you to ensure that the client is updating the version of the data that the client retrieved.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @param <C> CAS type
 * @author Morten Haraldsen
 */
public interface CasKeyValueDb<K extends Key<K>, V, C extends Comparable<C>> extends KeyValueDb<K, V>
{
    CasHolder<K, V, C> getCas(K key);

    void putCas(CasHolder<K, V, C> cas);
}
