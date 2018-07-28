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

package org.springframework.kafka.core.reactive;

import java.util.function.Function;

import org.apache.kafka.clients.producer.Producer;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;


/**
 * Interface for executing reactive kafka operations on producer side.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Mark Norkin
 */
public interface ReactiveKafkaProducerOperations<K, V> {

	<T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records);

	<T> Flux<SenderResult<T>> sendTransactionally(Publisher<? extends SenderRecord<K, V, T>> records);

	TransactionManager transactionManager();

	<T> Mono<T> doOnProducer(Function<Producer<K, V>, ? extends T> action);

}
