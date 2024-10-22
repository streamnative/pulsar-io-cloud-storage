/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jcloud.utils;

import java.io.IOException;
import org.apache.pulsar.io.jcloud.util.HexStringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * hex string utils unit tests.
 */
public class HexStringUtilsTest {
    @Test
    public void testConvertHexStringToBytes() throws IOException {
        String hexString = "0x10";
        byte[] expectedBytes = {0x10};
        Assert.assertArrayEquals(expectedBytes, HexStringUtils.convertHexStringToBytes(hexString));

        hexString = "0x102030405060";
        expectedBytes = new byte[]{0x10, 0x20, 0x30, 0x40, 0x50, 0x60};
        Assert.assertArrayEquals(expectedBytes, HexStringUtils.convertHexStringToBytes(hexString));

        hexString = "0xfffff";
        expectedBytes = new byte[]{(byte) 0x0f, (byte) 0xff, (byte) 0xff};
        Assert.assertArrayEquals(expectedBytes, HexStringUtils.convertHexStringToBytes(hexString));
    }
}
