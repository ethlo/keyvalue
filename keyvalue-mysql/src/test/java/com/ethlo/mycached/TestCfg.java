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

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ethlo.keyvalue.mysql.MysqlClientManagerImpl;

@EnableAutoConfiguration
@Configuration
public class TestCfg
{
    @Bean
    public MysqlClientManagerImpl legacyMyCachedClientManager(DataSource dataSource)
    {
        return new MysqlClientManagerImpl(dataSource);
    }
}