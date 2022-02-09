/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.listener;

import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * A batch error handler used by the default error handler when the listener does
 * not throw a {@link BatchListenerFailedException}.
 *
 * @author Gary Russell
 * @since 2.8.3
 *
 */
@SuppressWarnings("deprecation")
class FallbackBatchErrorHandler extends RetryingBatchErrorHandler {

	/**
	 * Construct an instance with a default {@link FixedBackOff} (unlimited attempts with
	 * a 5 second back off).
	 */
	FallbackBatchErrorHandler() {
		super();
	}

	/**
	 * Construct an instance with the provided {@link BackOff} and
	 * {@link ConsumerRecordRecoverer}. If the recoverer is {@code null}, the discarded
	 * records (topic-partition{@literal @}offset) will be logged.
	 * @param backOff the back off.
	 * @param recoverer the recoverer.
	 */
	FallbackBatchErrorHandler(BackOff backOff, @Nullable ConsumerRecordRecoverer recoverer) {
		super(backOff, recoverer);
	}

}
