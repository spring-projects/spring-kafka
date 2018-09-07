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

package org.springframework.kafka.support;

/**
 * Utilities for supporting transactions.
 *
 * @author Gary Russell
 * @since 1.3.7
 *
 */
public final class TransactionSupport {

	private static final ThreadLocal<String> transactionIdSuffix = new ThreadLocal<>();

	private TransactionSupport() {
		super();
	}

	public static void setTransactionIdSuffix(String suffix) {
		transactionIdSuffix.set(suffix);
	}

	public static String getTransactionIdSuffix() {
		return transactionIdSuffix.get();
	}

	public static void clearTransactionIdSuffix() {
		transactionIdSuffix.remove();
	}

}
