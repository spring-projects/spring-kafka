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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;

/**
 * Reactive kafka consumer operations implementation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Mark Norkin
 */
public class ReactiveKafkaConsumerTemplate<K, V> implements ReactiveKafkaConsumerOperations<K, V> {
	private final KafkaReceiver<K, V> kafkaReceiver;

	public ReactiveKafkaConsumerTemplate(ReceiverOptions<K, V> receiverOptions) {
		this.kafkaReceiver = KafkaReceiver.create(receiverOptions);
	}

	@Override
	public Flux<ReceiverRecord<K, V>> receive() {
		return this.kafkaReceiver.receive();
	}

	@Override
	public Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck() {
		return this.kafkaReceiver.receiveAutoAck();
	}

	@Override
	public Flux<ConsumerRecord<K, V>> receiveAtmostOnce() {
		return this.kafkaReceiver.receiveAtmostOnce();
	}

	@Override
	public Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager) {
		return this.kafkaReceiver.receiveExactlyOnce(transactionManager);
	}

	@Override
	public <T> Mono<T> doOnConsumer(Function<Consumer<K, V>, ? extends T> function) {
		return this.kafkaReceiver.doOnConsumer(function);
	}
}
