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

import org.springframework.dao.DataAccessResourceFailureException;

import com.ethlo.keyvalue.KeyValueDbManager;
import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;

public class MysqlClientManagerImpl extends KeyValueDbManager<MysqlClient>
{
    private final MysqlUtil mysqlUtil;
    private final DataSource dataSource;

    public MysqlClientManagerImpl(DataSource dataSource)
    {
        this.dataSource = dataSource;
        this.mysqlUtil = new MysqlUtil(dataSource);
    }

    @Override
    public Object doCreateDb(String tableName, boolean allowCreate, KeyEncoder keyEncoder, DataCompressor dataCompressor)
    {
        if (!allowCreate && !this.mysqlUtil.tableExists(tableName))
        {
            throw new DataAccessResourceFailureException("No such database: " + tableName);
        }
        else
        {
            this.mysqlUtil.createTable(tableName);
        }
        return new MysqlClientImpl(tableName, dataSource, keyEncoder, dataCompressor);
    }
}
