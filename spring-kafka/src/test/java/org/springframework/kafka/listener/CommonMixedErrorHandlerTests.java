/*
 * Copyright 2021-2022 the original author or authors.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.8
 *
 */
public class CommonMixedErrorHandlerTests {

	@Test
	void testMixed() {
		CommonErrorHandler record = mock(CommonErrorHandler.class);
		CommonErrorHandler batch = mock(CommonErrorHandler.class);
		CommonMixedErrorHandler mixed = new CommonMixedErrorHandler(record, batch);
		mixed.handleBatch(null, null, null, null, null);
		verify(batch).handleBatch(null, null, null, null, null);
		mixed.handleOne(null, null, null, null);
		verify(record).handleOne(null, null, null, null);
		mixed.handleRemaining(null, null, null, null);
		verify(record).handleRemaining(null, null, null, null);
		mixed.handleOtherException(null, null, null, false);
		verify(record).handleOtherException(null, null, null, false);
		mixed.handleOtherException(null, null, null, true);
		verify(batch).handleOtherException(null, null, null, true);
		verifyNoMoreInteractions(record);
		verifyNoMoreInteractions(batch);
	}

}
