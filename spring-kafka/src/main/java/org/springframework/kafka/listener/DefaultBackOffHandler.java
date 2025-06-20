/*
 * Copyright 2022-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

/**
 * Default {@link BackOffHandler}; suspends the thread for the back off. If a container is
 * provided, {@link ListenerUtils#stoppableSleep(MessageListenerContainer, long)} is used,
 * to terminate the suspension if the container is stopped.
 *
 * @author Jan Marincek
 * @author Gary Russell
 * @since 2.9
 */
public class DefaultBackOffHandler implements BackOffHandler {

	@Override
	public void onNextBackOff(@Nullable MessageListenerContainer container, @Nullable Exception exception, long nextBackOff) {
		try {
			if (container == null) {
				Thread.sleep(nextBackOff);
			}
			else {
				ListenerUtils.stoppableSleep(container, nextBackOff);
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
