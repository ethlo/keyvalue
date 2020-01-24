package com.ethlo.mycached;

/*-
 * #%L
 * Key/value MySQL implementation
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

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.ethlo.keyvalue.MutatingKeyValueDb;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;
import com.ethlo.keyvalue.mysql.MysqlClientManagerImpl;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestCfg.class, properties = "spring.datasource.type=com.zaxxer.hikari.HikariDataSource")
//@Transactional
public abstract class AbstractTest
{
    protected final KeyEncoder keyEncoder = new HexKeyEncoder();
    protected final DataCompressor dataCompressor = new NopDataCompressor();

    protected MutatingKeyValueDb<ByteArrayKey, byte[]> mutatingKeyValueDb;
    protected CasKeyValueDb<ByteArrayKey, byte[], Long> casKeyValueDb;

    @Autowired
    private MysqlClientManagerImpl clientManager;

    @SuppressWarnings("unchecked")
    @Before
    public void setup()
    {
        final String dbName = "_kvtest";
        this.mutatingKeyValueDb = (MutatingKeyValueDb<ByteArrayKey, byte[]>) clientManager.createMainDb(dbName, true, keyEncoder, dataCompressor);
        this.casKeyValueDb = (CasKeyValueDb<ByteArrayKey, byte[], Long>) mutatingKeyValueDb;
    }
}
