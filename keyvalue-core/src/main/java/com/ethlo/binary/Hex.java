package com.ethlo.binary;

/*-
 * #%L
 * keyvalue-binary
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

public class Hex
{
    private static final char[] UPPERCASE_CHARACTERS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static final char[] LOWERCASE_CHARACTERS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private Hex()
    {
    }

    private static String encode(byte[] input, boolean uppercase)
    {
        final char[] hexDigits = new char[input.length * 2];
        int index = 0;
        for (byte num : input)
        {
            final int firstNibble = (num >> 4) & 0xF;
            final int secondNibble = num & 0xF;
            hexDigits[index++] = uppercase ? UPPERCASE_CHARACTERS[firstNibble] : LOWERCASE_CHARACTERS[firstNibble];
            hexDigits[index++] = uppercase ? UPPERCASE_CHARACTERS[secondNibble] : LOWERCASE_CHARACTERS[secondNibble];
        }
        return new String(hexDigits);
    }

    public static String encodeLowerCase(byte[] input)
    {
        return encode(input, false);
    }

    public static String encodeUpperCase(byte[] input)
    {
        return encode(input, true);
    }

    public static byte[] decode(String hex)
    {
        if (hex.length() % 2 != 0)
        {
            throw new IllegalArgumentException("Requires even number of characters, got " + hex.length());
        }
        final char[] chars = hex.toCharArray();
        final byte[] result = new byte[chars.length / 2];
        int index = 0;
        for (int i = 0; i < chars.length; i += 2)
        {
            result[index++] = hexToByte(chars, i);
        }
        return result;
    }

    private static byte hexToByte(final char[] data, final int offset)
    {
        final int firstDigit = toDigit(data[offset]);
        final int secondDigit = toDigit(data[offset + 1]);
        return (byte) ((firstDigit << 4) + secondDigit);
    }

    private static int toDigit(char hexChar)
    {
        return Character.digit(hexChar, 16);
    }
}
