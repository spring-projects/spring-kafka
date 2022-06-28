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

import java.time.Duration;

import org.apache.kafka.common.TopicPartition;

import org.springframework.lang.Nullable;

/**
 * A {@link BackOffHandler} that pauses the container for the backoff.
 *
 * @author Gary Russell
 * @since 2.9
 *
 */
public class ContainerPausingBackOffHandler implements BackOffHandler {

	private final DefaultBackOffHandler defaultBackOffHandler = new DefaultBackOffHandler();

	private final ListenerContainerPauseService pauser;

	/**
	 * Create an instance with the provided {@link ListenerContainerPauseService}.
	 * @param pauser the pause service.
	 */
	public ContainerPausingBackOffHandler(ListenerContainerPauseService pauser) {
		this.pauser = pauser;
	}

	@Override
	public void onNextBackOff(@Nullable MessageListenerContainer container, Exception exception, long nextBackOff) {
		if (container == null) {
			this.defaultBackOffHandler.onNextBackOff(container, exception, nextBackOff);
		}
		else {
			this.pauser.pause(container, Duration.ofMillis(nextBackOff));
		}
	}

	@Override
	public void onNextBackOff(MessageListenerContainer container, TopicPartition partition, long nextBackOff) {
		this.pauser.pausePartition(container, partition, Duration.ofMillis(nextBackOff));
	}

}
