package org.apache.rya.indexing;

import org.apache.accumulo.core.data.Value;

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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Utility methods for generating hashes. Note that MD5 is 16 bytes, or 32 Hex chars. To make it smaller (but still printable), this class
 * Base64 encodes those 16 bytes into 22 chars.
 */
public class Md5Hash {
    public static String md5Base64(final byte[] data) {
        return Base64.encodeBase64URLSafeString(DigestUtils.md5(data));
    }

    public static String md5Base64(final String string) {
        return md5Base64(StringUtils.getBytesUtf8(string));
    }

    public static byte[] md5Binary(final Value value) {
        return DigestUtils.md5(value.get());
    }
}
