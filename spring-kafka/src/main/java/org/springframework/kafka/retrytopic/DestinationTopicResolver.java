/*
 * Copyright 2016-2021 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import java.util.Map;

/**
 *
 * Contains methods for resolving the destination to which a message that failed
 * to be processed should be forwarded to, based on the current topic, the attempt
 * and the thrown exception.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public interface DestinationTopicResolver {

	DestinationTopic resolveNextDestination(String topic, Integer attempt, Exception e, long originalTimestamp);
	long resolveDestinationNextExecutionTimestamp(String topic, Integer attempt, Exception e, long originalTimestamp);
	DestinationTopic getCurrentTopic(String topic);
	void addDestinations(Map<String, DestinationTopicResolver.DestinationsHolder> sourceDestinationMapToAdd);

	static DestinationsHolder holderFor(DestinationTopic sourceDestination, DestinationTopic nextDestination) {
		return new DestinationsHolder(sourceDestination, nextDestination);
	}

	class DestinationsHolder {

		private final DestinationTopic sourceDestination;

		private final DestinationTopic nextDestination;

		DestinationsHolder(DestinationTopic sourceDestination, DestinationTopic nextDestination) {
			this.sourceDestination = sourceDestination;
			this.nextDestination = nextDestination;
		}

		protected DestinationTopic getNextDestination() {
			return this.nextDestination;
		}

		protected DestinationTopic getSourceDestination() {
			return this.sourceDestination;
		}
	}
}
