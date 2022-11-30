package com.ethlo.keyvalue.mysql;

/*-
 * #%L
 * Key-Value - MySQL implementation
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

import com.ethlo.keyvalue.BatchKeyValueDb;
import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.MutableKeyValueDb;
import com.ethlo.keyvalue.cas.BatchCasKeyValueDb;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;

public interface MysqlClient extends MutableKeyValueDb<ByteArrayKey, byte[]>,
        IterableKeyValueDb<ByteArrayKey, byte[]>,
        BatchKeyValueDb<ByteArrayKey, byte[]>,
        BatchCasKeyValueDb<ByteArrayKey, byte[], Long>,
        CasKeyValueDb<ByteArrayKey, byte[], Long>
{
}
