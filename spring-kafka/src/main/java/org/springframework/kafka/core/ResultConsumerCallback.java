package org.springframework.kafka.core;

import java.util.function.Consumer;

import io.confluent.parallelconsumer.ParallelStreamProcessor.ConsumeProduceResult;

public interface ResultConsumerCallback<K, V> {
	Consumer<ConsumeProduceResult<K, V, K, V>> resultConsumer(ConsumeProduceResult<K, V, K, V> result);
}
