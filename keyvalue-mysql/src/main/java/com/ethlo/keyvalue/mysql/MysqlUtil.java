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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.Assert;

public class MysqlUtil
{
    public static final int TABLE_NAME_MAX_LENGTH = 64;

    private final DataSource dataSource;
    private final NamedParameterJdbcTemplate tpl;

    public MysqlUtil(DataSource dataSource)
    {
        this.dataSource = dataSource;
        this.tpl = new NamedParameterJdbcTemplate(dataSource);
    }

    public void setup(String dbName, boolean allowCreate)
    {
        Assert.isTrue(dbName.length() <= TABLE_NAME_MAX_LENGTH, "dbName " + dbName + " is too long. Maximum is " + TABLE_NAME_MAX_LENGTH + " characters");

        if (tableExists(dbName) || allowCreate)
        {
            createTable(dbName);
        }
        else
        {
            throw new UncheckedIOException(new IOException("The database table " + dbName + " does not exist and allowCreate is false"));
        }
    }

    public void createTable(String dbName)
    {
        Assert.hasLength(dbName, "dbName cannot be empty");
        tpl.update("CREATE TABLE IF NOT EXISTS " + dbName + "(" +
                "mkey VARBINARY(255) NOT NULL PRIMARY KEY, " +
                "mvalue MEDIUMBLOB NOT NULL, " +
                "cas_column INT NOT NULL, " +
                "expire_time_column INT, " +
                "flags INT) " +
                "ENGINE=INNODB", new TreeMap<String, String>());
    }

    boolean tableExists(String dbName)
    {
        try (final Connection c = this.dataSource.getConnection())
        {
            final DatabaseMetaData md = c.getMetaData();
            final ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next())
            {
                final String tableName = rs.getString(3);
                if (dbName.equals(tableName))
                {
                    return true;
                }
            }
            return false;
        }
        catch (SQLException e)
        {
            throw new UncheckedIOException(new IOException(e));
        }
    }

}
