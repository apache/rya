package mvm.rya.indexing.accumulo;

/*
 * #%L
 * mvm.rya.indexing.accumulo
 * %%
 * Copyright (C) 2014 Rya
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

import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Utility methods for generating hashes. Note that MD5 is 16 bytes, or 32 Hex chars. To make it smaller (but still printable), this class
 * Base64 encodes those 16 bytes into 22 chars.
 */
public class Md5Hash {
    public static String md5Base64(byte[] data) {
        return Base64.encodeBase64URLSafeString(DigestUtils.md5(data));
    }

    public static String md5Base64(String string) {
        return md5Base64(StringUtils.getBytesUtf8(string));
    }

	public static byte[] md5Binary(Value value) {
        return DigestUtils.md5(value.get());
	}
}
