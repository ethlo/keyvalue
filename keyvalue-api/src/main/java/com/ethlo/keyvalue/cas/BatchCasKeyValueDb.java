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

import java.util.List;

import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.keys.Key;


/**
 * Extension of {@link CasKeyValueDb} that allows to do batched writes.
 * 
 * @author Morten Haraldsen
 * 
 */
public interface BatchCasKeyValueDb<K extends Key,V,C extends Comparable<C>> extends CasKeyValueDb<K,V,C>
{
	void putAll(List<CasHolder<K,V,C>> casList);
}
