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

import java.util.function.Consumer;

import io.confluent.parallelconsumer.ParallelStreamProcessor.ConsumeProduceResult;

/**
 * This interface is an interface that marks whether there is a Callback for {@link ConsumeProduceResult}.
 * Users should implement one of {@link Poll}, {@link PollAndProduce}, {@link PollAndProduceResult}
 * , {@link PollAndProduceMany}, {@link PollAndProduceManyResult} instead of {@link ParallelConsumerResultInterface}.
 *
 * @author Sanghyeok An
 *
 * @since 3.3
 */

public interface ParallelConsumerResultInterface<K, V> {
	Consumer<ConsumeProduceResult<K, V, K, V>> resultConsumer(ConsumeProduceResult<K, V, K, V> result);
}
