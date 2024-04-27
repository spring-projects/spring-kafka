/*
 * Copyright 2014-2024 the original author or authors.
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

package org.springframework.kafka.core.parallelconsumer;

import org.springframework.kafka.config.ParallelConsumerContext;
import org.springframework.kafka.core.ParallelConsumerFactory;

import io.confluent.parallelconsumer.PollContext;

/**
 * User should create ConcreteClass of this and register it as Spring Bean.
 * Concrete class of ParallelConsumerCallback will be registered {@link ParallelConsumerContext},
 * and then it will be used in {@link ParallelConsumerFactory} when ParallelConsumerFactory start.
 *
 * @author Sanghyeok An
 * @since 3.3
 */

public interface Poll<K, V> extends ParallelConsumerCallback<K, V> {

	/**
	 * This is for {@link ParallelConsumerFactory} and {@link ParallelConsumerContext}.
	 * ParallelConsumer will process the consumed messages using this callback.
	 * @param context context which Parallel Consumer produce
	 * @return void.
	 */
	void accept(PollContext<K, V> context);

}
