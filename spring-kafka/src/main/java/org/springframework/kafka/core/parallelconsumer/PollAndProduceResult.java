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
import java.util.function.Function;

import org.springframework.kafka.core.ParallelConsumerFactory;

import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.ParallelStreamProcessor.ConsumeProduceResult;
import io.confluent.parallelconsumer.PollContext;

/**
 * This interface is intended for use when the user use the producer after consuming.
 * User should implement {@link PollAndProduceResult} and register it as Spring Bean.
 * Both {@link PollAndProduceResult#accept(PollContext)} {@link PollAndProduceResult#resultConsumer(ConsumeProduceResult)}
 * will be called by {@link ParallelStreamProcessor#pollAndProduce(Function, Consumer)}
 * when {@link ParallelConsumerFactory} started.
 *
 * @author Sanghyeok An
 *
 * @since 3.3
 */

public interface PollAndProduceResult<K, V> extends PollAndProduce<K, V>,
													ParallelConsumerResultInterface<K, V> {

}
