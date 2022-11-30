package com.ethlo.keyvalue.mysql;

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

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = TestCfg.class)
public abstract class AbstractTest
{
    protected final KeyEncoder keyEncoder = new HexKeyEncoder();
    protected final DataCompressor dataCompressor = new NopDataCompressor();
    protected MysqlClient db;
    @Autowired
    private DataSource dataSource;

    @BeforeEach
    public void setup()
    {
        final MysqlClientManagerImpl<MysqlClient> clientManager = new MysqlClientManagerImpl<>(dataSource);
        this.db = clientManager.getDb("_kvtest", true, keyEncoder, dataCompressor);
        this.db.clear();
    }
}
