package com.ethlo.keyvalue.compression;

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

public class NopDataCompressor implements DataCompressor
{
    @Override
    public byte[] compress(byte[] uncompressed)
    {
        return uncompressed;
    }

    @Override
    public byte[] decompress(byte[] compressed)
    {
        return compressed;
    }

    @Override
    public int getType()
    {
        return 0;
    }
}
