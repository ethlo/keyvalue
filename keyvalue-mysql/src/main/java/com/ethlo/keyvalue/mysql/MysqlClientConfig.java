package com.ethlo.keyvalue.mysql;

/*-
 * #%L
 * Key-Value - MySQL implementation
 * %%
 * Copyright (C) 2013 - 2023 Morten Haraldsen (ethlo)
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

import com.ethlo.keyvalue.ClientConfig;
import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;

public class MysqlClientConfig extends ClientConfig
{
    private final int batchSize;

    public MysqlClientConfig(final String name, final KeyEncoder keyEncoder, final DataCompressor dataCompressor, final int batchSize)
    {
        super(name, keyEncoder, dataCompressor);
        this.batchSize = batchSize;
    }

    public static MysqlClientConfig withName(final String name)
    {
        return new MysqlClientConfig(name, new HexKeyEncoder(), new NopDataCompressor(), 10);
    }

    public int getBatchSize()
    {
        return batchSize;
    }
}
