/*
 * Copyright 2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.7.1
 *
 */
public class ListenerUtilsTests {

	@Test
	void stoppableSleep() throws InterruptedException {
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		long t1 = System.currentTimeMillis();
		ListenerUtils.stoppableSleep(container, 500);
		assertThat(System.currentTimeMillis() - t1).isGreaterThanOrEqualTo(500);
	}

}
