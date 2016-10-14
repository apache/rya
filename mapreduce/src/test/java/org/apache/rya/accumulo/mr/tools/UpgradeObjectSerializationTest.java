package mvm.rya.accumulo.mr.tools;

/*
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



import mvm.rya.api.resolver.impl.*;
import org.junit.Test;

import static mvm.rya.accumulo.mr.tools.Upgrade322Tool.UpgradeObjectSerialization;
import static org.junit.Assert.*;

public class UpgradeObjectSerializationTest {

    @Test
    public void testBooleanUpgrade() throws Exception {
        String object = "true";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, BooleanRyaTypeResolver.BOOLEAN_LITERAL_MARKER);

        assertEquals("1", upgrade);
    }

    @Test
    public void testBooleanUpgradeFalse() throws Exception {
        String object = "false";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, BooleanRyaTypeResolver.BOOLEAN_LITERAL_MARKER);

        assertEquals("0", upgrade);
    }

    @Test
    public void testByteUpgradeLowest() throws Exception {
        String object = "-127";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, ByteRyaTypeResolver.LITERAL_MARKER);

        assertEquals("81", upgrade);
    }

    @Test
    public void testByteUpgradeHighest() throws Exception {
        String object = "127";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, ByteRyaTypeResolver.LITERAL_MARKER);

        assertEquals("7f", upgrade);
    }

    @Test
    public void testLongUpgrade() throws Exception {
        String object = "00000000000000000010";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, LongRyaTypeResolver.LONG_LITERAL_MARKER);

        assertEquals("800000000000000a", upgrade);
    }

    @Test
    public void testIntUpgrade() throws Exception {
        String object = "00000000010";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, IntegerRyaTypeResolver.INTEGER_LITERAL_MARKER);

        assertEquals("8000000a", upgrade);
    }

    @Test
    public void testDateTimeUpgrade() throws Exception {
        String object = "9223370726404375807";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, DateTimeRyaTypeResolver.DATETIME_LITERAL_MARKER);

        assertEquals("800001311cee3b00", upgrade);
    }

    @Test
    public void testDoubleUpgrade() throws Exception {
        String object = "00001 1.0";
        final UpgradeObjectSerialization upgradeObjectSerialization
          = new UpgradeObjectSerialization();
        final String upgrade = upgradeObjectSerialization
          .upgrade(object, DoubleRyaTypeResolver.DOUBLE_LITERAL_MARKER);

        assertEquals("c024000000000000", upgrade);
    }
}
