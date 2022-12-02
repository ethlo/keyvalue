package com.ethlo.keyvalue.keys;

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import com.ethlo.binary.Hex;

public class ByteArrayKey extends Key<ByteArrayKey>
{
    private byte[] keyData;

    public ByteArrayKey()
    {

    }

    public ByteArrayKey(byte[] keyData)
    {
        if (keyData == null)
        {
            throw new IllegalArgumentException("keyData cannot be null");
        }
        this.keyData = keyData;
    }

    public static ByteArrayKey of(int... bytes)
    {
        final byte[] result = new byte[bytes.length];
        for (int i = 0; i < result.length; i++)
        {
            result[i] = (byte) bytes[i];
        }
        return new ByteArrayKey(result);
    }

    @Override
    public boolean equals(Object b)
    {
        if (b == this)
        {
            return true;
        }
        else if (b == null || b.getClass() != this.getClass())
        {
            return false;
        }
        return Arrays.equals(keyData, ((ByteArrayKey) b).keyData);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(keyData);
    }

    public byte[] getByteArray()
    {
        return keyData;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + Hex.encodeUpperCase(keyData) + "]";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(keyData.length);
        out.write(keyData);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException
    {
        keyData = new byte[in.readInt()];
        in.readFully(keyData);
    }

    @Override
    public int compareTo(ByteArrayKey b)
    {
        return Arrays.compare(keyData, b.keyData);
    }
}
