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


import java.io.EOFException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.util.CloseableIterator;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.BatchWriteWrapper;
import com.ethlo.keyvalue.CloseableAbstractIterator;
import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;

/**
 * Works by using standard SQL for handling data operations instead of the MySql-MemCached interface
 *
 * @author Morten Haraldsen
 */
public class MysqlClientImpl implements MysqlClient
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlClientImpl.class);

    private final JdbcTemplate tpl;

    private final KeyEncoder keyEncoder;
    private final DataCompressor dataCompressor;

    private final String getCasSqlPrefix;
    private final String getCasSqlFirst;
    private final String deleteSql;
    private final String insertOnDuplicateUpdateSql;

    private final RowMapper<byte[]> rowMapper;
    private final RowMapper<CasHolder<ByteArrayKey, byte[], Long>> casRowMapper;

    private final PreparedStatementCreatorFactory getDataCasPscFactory;
    private final PreparedStatementCreatorFactory clearPscFactory;
    private final PreparedStatementCreatorFactory insertCasPscFactory;
    private final PreparedStatementCreatorFactory updateCasPscFactory;
    private final PreparedStatementCreatorFactory getDataPscFactory;
    private final int iteratorBatchSize;

    public MysqlClientImpl(final MysqlClientConfig config, final DataSource dataSource)
    {
        this.keyEncoder = config.getKeyEncoder();
        this.dataCompressor = config.getDataCompressor();
        this.iteratorBatchSize = config.getBatchSize();
        final String tableName = config.getName();

        Assert.hasLength(tableName, "tableName cannot be null");
        Assert.notNull(dataSource, "dataSource cannot be null");
        this.tpl = new JdbcTemplate(dataSource);

        String getSql = "SELECT mvalue FROM " + tableName + " WHERE mkey = ?";
        String getCasSql = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey = ?";
        this.getCasSqlFirst = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey > ? ORDER BY mkey";
        this.getCasSqlPrefix = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey > ? AND mkey LIKE ? ORDER BY mkey";
        String insertCasSql = "INSERT INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, ?)";
        String updateCasSql = "UPDATE " + tableName + " SET mvalue = ?, cas_column = cas_column + 1 WHERE mkey = ? AND cas_column = ?";
        this.insertOnDuplicateUpdateSql = "INSERT INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE mvalue=?, cas_column=cas_column+1";
        this.deleteSql = "DELETE FROM " + tableName + " WHERE mkey = ?";
        String clearSql = "DELETE FROM " + tableName;

        this.rowMapper = (rs, rowNum) ->
        {
            final byte[] data = rs.getBytes(1);
            return dataCompressor.decompress(data);
        };

        this.casRowMapper = (rs, rowNum) ->
        {
            final ByteArrayKey key = new ByteArrayKey(keyEncoder.fromString(rs.getString(1)));
            final byte[] data = rs.getBytes(2);
            final byte[] value = dataCompressor.decompress(data);
            final long cas = rs.getLong(3);
            return new CasHolder<>(cas, key, value);
        };

        getDataPscFactory = new PreparedStatementCreatorFactory(getSql, Types.VARCHAR);
        getDataCasPscFactory = new PreparedStatementCreatorFactory(getCasSql, Types.VARCHAR);
        clearPscFactory = new PreparedStatementCreatorFactory(clearSql);
        insertCasPscFactory = new PreparedStatementCreatorFactory(insertCasSql, Types.VARCHAR, Types.BINARY, Types.INTEGER);
        updateCasPscFactory = new PreparedStatementCreatorFactory(updateCasSql, Types.BINARY, Types.VARCHAR, Types.INTEGER);
    }

    @Override
    public byte[] get(final ByteArrayKey key)
    {
        return getData(key, getDataPscFactory, rowMapper);
    }

    private <T> T getData(final ByteArrayKey key, final PreparedStatementCreatorFactory factory, final RowMapper<T> rowMapper)
    {
        final String strKey = keyEncoder.toString(key.getByteArray());
        final PreparedStatementCreator getPsc = factory.newPreparedStatementCreator(Collections.singletonList(strKey));
        return DataAccessUtils.singleResult(tpl.query(getPsc, rowMapper));
    }

    @Override
    public void put(final ByteArrayKey key, final byte[] value)
    {
        putAll(Collections.singletonMap(key, value));
    }

    @Override
    public void putAll(final Map<ByteArrayKey, byte[]> values)
    {
        insertOnDuplicateKeyUpdate(values);
    }

    private void insertOnDuplicateKeyUpdate(final Map<ByteArrayKey, byte[]> values)
    {
        final List<ByteArrayKey> keys = new ArrayList<>(values.keySet());
        int[] updateCounts = tpl.batchUpdate(insertOnDuplicateUpdateSql, new BatchPreparedStatementSetter()
        {
            public void setValues(@NonNull PreparedStatement ps, int i) throws SQLException
            {
                final ByteArrayKey key = keys.get(i);
                final byte[] value = values.get(key);
                final byte[] compressedValue = dataCompressor.compress(value);
                ps.setString(1, keyEncoder.toString(key.getByteArray()));
                ps.setBytes(2, compressedValue);
                ps.setLong(3, 0L);
                ps.setBytes(4, compressedValue);
            }

            public int getBatchSize()
            {
                return values.size();
            }
        });

        logger.debug("Updated {} entries", updateCounts.length);
    }

    @Override
    public void delete(ByteArrayKey key)
    {
        tpl.update(deleteSql, keyEncoder.toString(key.getByteArray()));
    }

    @Override
    public void clear()
    {
        tpl.update(clearPscFactory.newPreparedStatementCreator(new Object[0]));
    }

    @Override
    public void close()
    {
        // Nothing to close
    }

    @Override
    public CasHolder<ByteArrayKey, byte[], Long> getCas(final ByteArrayKey key)
    {
        return getData(key, getDataCasPscFactory, casRowMapper);
    }

    @Override
    public void putCas(final CasHolder<ByteArrayKey, byte[], Long> cas)
    {
        if (cas.version() != null)
        {
            updateWithNonNullCasValue(cas);
        }
        else
        {
            insertNewDueToNullCasValue(cas);
        }
    }

    private void insertNewDueToNullCasValue(final CasHolder<ByteArrayKey, byte[], Long> cas)
    {
        final String strKey = keyEncoder.toString(cas.key().getByteArray());
        final byte[] value = dataCompressor.compress(cas.value());
        final PreparedStatementCreator psc = insertCasPscFactory.newPreparedStatementCreator(Arrays.asList(strKey, value, 1L));

        try
        {
            tpl.update(psc);
        }
        catch (DuplicateKeyException exc)
        {
            throw new OptimisticLockingFailureException("Cannot update " + cas.key(), exc);
        }
    }

    private void updateWithNonNullCasValue(final CasHolder<ByteArrayKey, byte[], Long> cas)
    {
        final String strKey = keyEncoder.toString(cas.key().getByteArray());
        final long casValue = cas.version();
        final byte[] value = dataCompressor.compress(cas.value());

        final int rowsChanged = tpl.update(updateCasPscFactory.newPreparedStatementCreator(Arrays.asList(value, strKey, casValue)));
        if (rowsChanged == 0)
        {
            throw new OptimisticLockingFailureException("Cannot update data for key " + cas.key() + " due to concurrent modification. Details: Attempted CAS value=" + cas.version());
        }
    }

    @Override
    public void putAll(final List<CasHolder<ByteArrayKey, byte[], Long>> casList)
    {
        // Hard to solve in any more efficient way as we need to compare the previous version with the new one
        for (CasHolder<ByteArrayKey, byte[], Long> cas : casList)
        {
            putCas(cas);
        }
    }

    @Override
    public byte[] mutate(ByteArrayKey key, UnaryOperator<byte[]> mutator)
    {
        final CasHolder<ByteArrayKey, byte[], Long> cas = this.getCas(key);
        final byte[] result = mutator.apply(cas != null ? cas.value() : null);

        if (cas != null)
        {
            this.putCas(cas.ofValue(result));
        }
        else
        {
            this.putCas(new CasHolder<>(null, key, result));
        }
        return result;
    }

    @Override
    public CloseableIterator<Map.Entry<ByteArrayKey, byte[]>> iterator()
    {
        final PreparedStatementCreator psc = con ->
                con.prepareStatement(getCasSqlFirst, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return getCloseableAbstractIterator(psc, new AbstractMap.SimpleEntry<>(1, ""), iteratorBatchSize);
    }

    @Override
    public CloseableIterator<Map.Entry<ByteArrayKey, byte[]>> iteratorFromPrefix(ByteArrayKey key)
    {
        final PreparedStatementCreator psc = con ->
        {
            final PreparedStatement ps = con.prepareStatement(getCasSqlPrefix, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            final String strKey = keyEncoder.toString(key.getByteArray());
            ps.setString(1, strKey + "%");
            return ps;
        };

        return getCloseableAbstractIterator(psc, new AbstractMap.SimpleEntry<>(2, ""), iteratorBatchSize);
    }

    private SqlCloseableAbstractIterator<Map.Entry<ByteArrayKey, byte[]>> createIterator(JdbcTemplate tpl, PreparedStatementCreator psc, Map.Entry<Integer, Serializable> startKey, int batchSize)
    {
        try
        {
            return new SqlCloseableAbstractIterator<>(Objects.requireNonNull(tpl.getDataSource()).getConnection(), psc, startKey, batchSize, (rs, num) ->
            {
                final CasHolder<ByteArrayKey, byte[], Long> res = casRowMapper.mapRow(rs, num);
                return new AbstractMap.SimpleEntry<>(Objects.requireNonNull(res).key(), res.value());
            });
        }
        catch (SQLException exc)
        {
            throw new DataAccessResourceFailureException(exc.getMessage(), exc);
        }
    }

    private CloseableAbstractIterator<Map.Entry<ByteArrayKey, byte[]>> getCloseableAbstractIterator(PreparedStatementCreator psc, final Map.Entry<Integer, Serializable> startKey, final int batchSize)
    {
        final AtomicReference<String> lastRetryKey = new AtomicReference<>();
        final AtomicReference<SqlCloseableAbstractIterator<Map.Entry<ByteArrayKey, byte[]>>> iterator = new AtomicReference<>();
        return new CloseableAbstractIterator<>()
        {
            @Override
            public void close()
            {
                Optional.ofNullable(iterator.get()).ifPresent(SqlCloseableAbstractIterator::close);
            }

            @Override
            protected Map.Entry<ByteArrayKey, byte[]> computeNext()
            {
                if (iterator.get() == null)
                {
                    iterator.set(createIterator(tpl, psc, startKey, batchSize));
                }
                final SqlCloseableAbstractIterator<Map.Entry<ByteArrayKey, byte[]>> iter = iterator.get();
                if (iter.hasNext())
                {
                    return iter.next();
                }
                else
                {
                    if (iter.exception != null)
                    {
                        return processExceptionState(iter);
                    }
                    return endOfData();
                }
            }

            private Map.Entry<ByteArrayKey, byte[]> processExceptionState(SqlCloseableAbstractIterator<Map.Entry<ByteArrayKey, byte[]>> iter)
            {
                iter.close();

                if (iter.exception.getCause() instanceof EOFException)
                {
                    // Connection was dropped, retry from last seen key

                    // 1. First check if we have already failed at this position
                    if (iter.lastSeenKey.equals(lastRetryKey.get()))
                    {
                        throw new DataAccessResourceFailureException("Unable to get past key " + lastRetryKey.get(), iter.exception);
                    }

                    // 2. Create new connection and try again
                    final String lastSeenKey = iter.lastSeenKey;
                    logger.warn("Starting new iteration from after key '{}'", lastSeenKey);
                    iterator.set(createIterator(tpl, psc, new AbstractMap.SimpleEntry<>(startKey.getKey(), lastSeenKey), batchSize));
                    lastRetryKey.set(lastSeenKey);
                    return iterator.get().computeNext();
                }
                throw new DataAccessResourceFailureException("An unexpected error occurred during iteration of data", iter.exception);
            }
        };
    }

    @Override
    public void putAll(final BatchWriteWrapper<ByteArrayKey, byte[]> batch)
    {
        this.putAll(batch.data());
    }

    private static class SqlCloseableAbstractIterator<T> extends CloseableAbstractIterator<T>
    {
        private final Connection connection;
        private final PreparedStatement ps;
        private final ResultSet rs;
        private final RowMapper<T> mapper;
        private SQLException exception;
        private String lastSeenKey;

        public SqlCloseableAbstractIterator(final Connection connection, final PreparedStatementCreator psc, Map.Entry<Integer, Serializable> startKey, final int batchSize, final RowMapper<T> mapper)
        {
            this.mapper = mapper;
            try
            {
                this.connection = Objects.requireNonNull(connection);
                ps = psc.createPreparedStatement(connection);
                ps.setFetchSize(batchSize);
                ps.setObject(startKey.getKey(), startKey.getValue());
                rs = ps.executeQuery();
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }

        @Override
        public void close()
        {
            tryClose(rs);
            tryClose(ps);
            tryClose(connection);
        }

        private void tryClose(AutoCloseable closeable)
        {
            if (closeable != null)
            {
                try
                {
                    closeable.close();
                }
                catch (Exception e)
                {
                    // Nothing more we can do
                }
            }
        }

        @Override
        protected T computeNext()
        {
            try
            {
                if (rs.next())
                {
                    lastSeenKey = rs.getString("mkey");
                    return mapper.mapRow(rs, rs.getRow());
                }
                close();
                return endOfData();
            }
            catch (SQLException exc)
            {
                logger.warn("SQL exception during fetching next row from result set: {}", exc.getMessage());
                close();
                this.exception = exc;
                return endOfData();
            }
        }
    }
}
