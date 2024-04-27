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

package org.springframework.kafka.core;

import java.util.List;
import java.util.regex.Pattern;

import io.confluent.parallelconsumer.PollContext;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.config.ParallelConsumerContext;

/**
 * User should create ConcreteClass of this and register it as Spring Bean.
 * Concrete class of ParallelConsumerCallback will be registered {@link ParallelConsumerContext},
 * and then it will be used in {@link ParallelConsumerFactory} when ParallelConsumerFactory start.
 * @author ...
 * @since 3.2.0
 */

public interface ParallelConsumerCallback<K, V> {

	/**
	 * ...
	 */
	List<String> getTopics();
	default Pattern getSubscribeTopicsPattern(){
		return null;
	}
	default ConsumerRebalanceListener getRebalanceListener(){
		return null;
	}
}
