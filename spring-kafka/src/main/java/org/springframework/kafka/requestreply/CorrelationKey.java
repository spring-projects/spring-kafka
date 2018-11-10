/*
 * Copyright 2018 the original author or authors.
 *
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
 */

package org.springframework.kafka.requestreply;

import java.math.BigInteger;
import java.util.Arrays;

import org.springframework.util.Assert;

/**
 * Wrapper for byte[] that can be used as a hash key. We could have used BigInteger
 * instead but this wrapper is much less expensive. We do use a BigInteger in
 * {@link #toString()} though.
 *
 * @author Gary Russell
 * @since 2.1.3
 */
public final class CorrelationKey {

	private final byte[] correlationId;

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

	@Override
	public String toString() {
		return "[" + new BigInteger(this.correlationId) + "]";
	}

}
