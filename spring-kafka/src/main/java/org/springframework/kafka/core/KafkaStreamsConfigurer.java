/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.kafka.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateRestoreListener;

/**
 * Wrapper class of configurable properties of {@link KafkaStreams}. Note that
 * {@code setters} of this configurer has precedence over {@link KafkaStreamsCustomizer}s
 *
 * @author Nurettin Yilmaz
 */
class KafkaStreamsConfigurer {

	private List<KafkaStreamsCustomizer> kafkaStreamsCustomizers = new ArrayList<>();

	private KafkaStreams.StateListener stateListener;

	private StateRestoreListener stateRestoreListener;

	private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

	KafkaStreamsConfigurer() {

	}

	void configure(KafkaStreams kafkaStreams) {
		kafkaStreamsCustomizers.forEach(kafkaStreamsCustomizer -> kafkaStreamsCustomizer.customize(kafkaStreams));

		Optional.ofNullable(this.stateListener).ifPresent(kafkaStreams::setStateListener);
		Optional.ofNullable(this.stateRestoreListener).ifPresent(kafkaStreams::setGlobalStateRestoreListener);
		Optional.ofNullable(this.uncaughtExceptionHandler).ifPresent(kafkaStreams::setUncaughtExceptionHandler);
	}

	void setStateListener(KafkaStreams.StateListener stateListener) {
		this.stateListener = stateListener;
	}

	void setStateRestoreListener(StateRestoreListener stateRestoreListener) {
		this.stateRestoreListener = stateRestoreListener;
	}

	void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
		this.uncaughtExceptionHandler = uncaughtExceptionHandler;
	}

	void addKafkaStreamsCustomizers(List<KafkaStreamsCustomizer> kafkaStreamsCustomizers) {
		this.kafkaStreamsCustomizers.addAll(kafkaStreamsCustomizers);
	}
}
