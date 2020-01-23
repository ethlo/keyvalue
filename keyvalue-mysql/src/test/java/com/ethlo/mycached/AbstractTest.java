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

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestCfg.class, properties = "spring.datasource.type=com.zaxxer.hikari.HikariDataSource")
@Transactional
public abstract class AbstractTest
{
    protected KeyEncoder keyEncoder = new HexKeyEncoder();
    protected DataCompressor dataCompressor = new NopDataCompressor();
}
