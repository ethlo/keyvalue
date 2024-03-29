package com.ethlo.keyvalue;

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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KeyValueDbManager<T extends BaseKeyValueDb> implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(KeyValueDbManager.class);
    private final Map<String, T> dbs = new HashMap<>();

    protected abstract T doCreateDb(final ClientConfig config);

    public T getDb(ClientConfig config)
    {
        T db = this.getOpenDb(config.getName());
        if (db == null)
        {
            final T rawDb = doCreateDb(config);
            dbs.put(config.getName(), rawDb);
            return rawDb;
        }
        return db;
    }

    private T getOpenDb(String name)
    {
        return this.dbs.get(name);
    }

    private void doClose(final T db)
    {
        try
        {
            db.close();
        }
        catch (Exception e)
        {
            logger.error("Cannot close " + db, e);
        }
    }

    @Override
    public void close()
    {
        for (T db : this.dbs.values())
        {
            doClose(db);
        }
    }

    public final List<String> listOpen()
    {
        return new ArrayList<>(this.dbs.keySet());
    }
}
