package com.ethlo.keyvalue;

/*-
 * #%L
 * Key-Value - MySQL implementation
 * %%
 * Copyright (C) 2013 - 2022 Morten Haraldsen (ethlo)
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

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.hashmap.HashmapKeyValueDbManager;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;
import com.ethlo.keyvalue.test.MutateKeyValueDbTest;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = TestCfg.class)
class StandardMutateTest extends MutateKeyValueDbTest
{
    @Autowired
    private HashmapKeyValueDbManager hashmapKeyValueDbManager;

    @Override
    protected MutableKeyValueDb<ByteArrayKey, byte[]> getMutableKeyValueDb()
    {
        return hashmapKeyValueDbManager.getDb(ClientConfig.withName("_kvtest"));
    }
}
