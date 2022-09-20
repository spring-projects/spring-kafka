/*
 * Copyright 2018-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.requestreply;

import java.util.Arrays;

import org.springframework.util.Assert;

/**
 * Wrapper for byte[] that can be used as a hash key. We could have used BigInteger
 * instead but this wrapper is much less expensive.
 *
 * @author Gary Russell
 * @since 2.1.3
 */
public final class CorrelationKey {

	private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

	private final byte[] correlationId;

	private String asString;

	private volatile Integer hashCode;

	public CorrelationKey(byte[] correlationId) { // NOSONAR array reference
		Assert.notNull(correlationId, "'correlationId' cannot be null");
		this.correlationId = correlationId; // NOSONAR array reference
	}

	public byte[] getCorrelationId() {
		return this.correlationId; // NOSONAR
	}

	@Override
	public int hashCode() {
		if (this.hashCode != null) {
			return this.hashCode;
		}
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.correlationId);
		this.hashCode = result;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CorrelationKey other = (CorrelationKey) obj;
		if (!Arrays.equals(this.correlationId, other.correlationId)) {
			return false;
		}
		return true;
	}

	private static String bytesToHex(byte[] bytes) {
		boolean uuid = bytes.length == 16;
		char[] hexChars = new char[bytes.length * 2 + (uuid ? 4 : 0)];
		int i = 0;
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[i++] = HEX_ARRAY[v >>> 4];
			hexChars[i++] = HEX_ARRAY[v & 0x0F];
			if (uuid && (j == 3 || j == 5 || j == 7 || j == 9)) {
				hexChars[i++] = '-';
			}
		}
		return new String(hexChars);
	}

	@Override
	public String toString() {
		if (this.asString == null) {
			this.asString = bytesToHex(this.correlationId);
		}
		return this.asString;
	}

}
