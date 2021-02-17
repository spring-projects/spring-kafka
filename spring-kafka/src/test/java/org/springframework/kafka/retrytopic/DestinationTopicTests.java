/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.BinaryExceptionClassifierBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
public class DestinationTopicTests {

	// KafkaOperations

	protected KafkaOperations<Object, Object> kafkaOperations1 =
			new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Collections.emptyMap()));

	protected KafkaOperations<Object, Object> kafkaOperations2 =
			new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Collections.emptyMap()));

	// Suffixes

	private final DestinationTopicPropertiesFactory.DestinationTopicSuffixes suffixes =
			new DestinationTopicPropertiesFactory.DestinationTopicSuffixes("", "");

	private final String retrySuffix = suffixes.getRetrySuffix();

	private final String dltSuffix = suffixes.getDltSuffix();

	private final long noTimeout = -1;

	private final long timeout = 1000;

	// MaxAttempts

	private final int maxAttempts = 3;

	// DestinationTopic Properties

	protected DestinationTopic.Properties mainTopicProps =
			new DestinationTopic.Properties(0, "", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.FAIL_ON_ERROR, kafkaOperations1, getShouldRetryOn(), noTimeout);

	protected DestinationTopic.Properties firstRetryProps =
			new DestinationTopic.Properties(1000, retrySuffix + "-1000", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.FAIL_ON_ERROR, kafkaOperations1, getShouldRetryOn(), noTimeout);

	protected DestinationTopic.Properties secondRetryProps =
			new DestinationTopic.Properties(2000, retrySuffix + "-2000", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.FAIL_ON_ERROR, kafkaOperations1, getShouldRetryOn(), noTimeout);

	protected DestinationTopic.Properties dltTopicProps =
			new DestinationTopic.Properties(0, dltSuffix, DestinationTopic.Type.DLT, 4, 1,
					DltStrategy.FAIL_ON_ERROR, kafkaOperations1, (a, e) -> false, noTimeout);

	protected List<DestinationTopic.Properties> allProps = Arrays
			.asList(mainTopicProps, firstRetryProps, secondRetryProps, dltTopicProps);

	protected DestinationTopic.Properties mainTopicProps2 =
			new DestinationTopic.Properties(0, "", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.ALWAYS_RETRY_ON_ERROR, kafkaOperations2, getShouldRetryOn(), timeout);

	protected DestinationTopic.Properties firstRetryProps2 =
			new DestinationTopic.Properties(1000, retrySuffix + "-0", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.ALWAYS_RETRY_ON_ERROR, kafkaOperations2, getShouldRetryOn(), timeout);

	protected DestinationTopic.Properties secondRetryProps2 =
			new DestinationTopic.Properties(1000, retrySuffix + "-1", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.ALWAYS_RETRY_ON_ERROR, kafkaOperations2, getShouldRetryOn(), timeout);

	protected DestinationTopic.Properties dltTopicProps2 =
			new DestinationTopic.Properties(0, dltSuffix, DestinationTopic.Type.DLT, 4, 1,
					DltStrategy.ALWAYS_RETRY_ON_ERROR, kafkaOperations2, (a, e) -> false, timeout);

	protected List<DestinationTopic.Properties> allProps2 = Arrays
			.asList(mainTopicProps2, firstRetryProps2, secondRetryProps2, dltTopicProps2);

	protected DestinationTopic.Properties mainTopicProps3 =
			new DestinationTopic.Properties(0, "", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.NO_DLT, kafkaOperations2, getShouldRetryOn(), timeout);

	protected DestinationTopic.Properties firstRetryProps3 =
			new DestinationTopic.Properties(1000, retrySuffix + "-0", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.NO_DLT, kafkaOperations2, getShouldRetryOn(), timeout);

	protected DestinationTopic.Properties secondRetryProps3 =
			new DestinationTopic.Properties(1000, retrySuffix + "-1", DestinationTopic.Type.RETRY, 4, 1,
					DltStrategy.NO_DLT, kafkaOperations2, getShouldRetryOn(), timeout);

	protected List<DestinationTopic.Properties> allProps3 = Arrays
			.asList(mainTopicProps3, firstRetryProps3, secondRetryProps3);


	// Holders

	protected final static String FIRST_TOPIC = "firstTopic";

	protected PropsHolder mainDestinationHolder = new PropsHolder(FIRST_TOPIC, mainTopicProps);

	protected PropsHolder firstRetryDestinationHolder = new PropsHolder(FIRST_TOPIC, firstRetryProps);

	protected PropsHolder secondRetryDestinationHolder = new PropsHolder(FIRST_TOPIC, secondRetryProps);

	protected PropsHolder dltDestinationHolder = new PropsHolder(FIRST_TOPIC, dltTopicProps);

	protected List<PropsHolder> allFirstDestinationsHolders = Arrays
			.asList(mainDestinationHolder, firstRetryDestinationHolder, secondRetryDestinationHolder, dltDestinationHolder);

	protected final static String SECOND_TOPIC = "secondTopic";

	protected PropsHolder mainDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, mainTopicProps2);

	protected PropsHolder firstRetryDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, firstRetryProps2);

	protected PropsHolder secondRetryDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, secondRetryProps2);

	protected PropsHolder dltDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, dltTopicProps2);

	protected List<PropsHolder> allSecondDestinationHolders = Arrays
			.asList(mainDestinationHolder2, firstRetryDestinationHolder2, secondRetryDestinationHolder2, dltDestinationHolder2);

	protected final static String THIRD_TOPIC = "thirdTopic";

	protected PropsHolder mainDestinationHolder3 =
			new PropsHolder(THIRD_TOPIC, mainTopicProps3);

	protected PropsHolder firstRetryDestinationHolder3 =
			new PropsHolder(THIRD_TOPIC, firstRetryProps3);

	protected PropsHolder secondRetryDestinationHolder3 =
			new PropsHolder(THIRD_TOPIC, secondRetryProps3);

	protected List<PropsHolder> allThirdDestinationHolders = Arrays
			.asList(mainDestinationHolder3, firstRetryDestinationHolder3, secondRetryDestinationHolder3);

	// DestinationTopics

	protected DestinationTopic mainDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + mainTopicProps.suffix(), mainTopicProps);

	protected DestinationTopic firstRetryDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + firstRetryProps.suffix(), firstRetryProps);

	protected DestinationTopic secondRetryDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + secondRetryProps.suffix(), secondRetryProps);

	protected DestinationTopic dltDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + dltTopicProps.suffix(), dltTopicProps);

	protected DestinationTopic noOpsDestinationTopic =
			new DestinationTopic(dltDestinationTopic.getDestinationName() + "-noOps",
					new DestinationTopic.Properties(dltTopicProps, "-noOps", DestinationTopic.Type.NO_OPS));

	protected List<DestinationTopic> allFirstDestinationsTopics = Arrays
			.asList(mainDestinationTopic, firstRetryDestinationTopic, secondRetryDestinationTopic, dltDestinationTopic);

	protected DestinationTopic mainDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + mainTopicProps2.suffix(), mainTopicProps2);

	protected DestinationTopic firstRetryDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + firstRetryProps2.suffix(), firstRetryProps2);

	protected DestinationTopic secondRetryDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + secondRetryProps2.suffix(), secondRetryProps2);

	protected DestinationTopic dltDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + dltTopicProps2.suffix(), dltTopicProps2);

	protected DestinationTopic noOpsDestinationTopic2 =
			new DestinationTopic(dltDestinationTopic2.getDestinationName() + "-noOps",
					new DestinationTopic.Properties(dltTopicProps2, "-noOps", DestinationTopic.Type.NO_OPS));

	protected List<DestinationTopic> allSecondDestinationTopics = Arrays
			.asList(mainDestinationTopic2, firstRetryDestinationTopic2, secondRetryDestinationTopic2, dltDestinationTopic2);

	protected DestinationTopic mainDestinationTopic3 =
			new DestinationTopic(THIRD_TOPIC + mainTopicProps3.suffix(), mainTopicProps3);

	protected DestinationTopic firstRetryDestinationTopic3 =
			new DestinationTopic(THIRD_TOPIC + firstRetryProps3.suffix(), firstRetryProps3);

	protected DestinationTopic secondRetryDestinationTopic3 =
			new DestinationTopic(THIRD_TOPIC + secondRetryProps3.suffix(), secondRetryProps3);

	protected DestinationTopic noOpsDestinationTopic3 =
			new DestinationTopic(secondRetryDestinationTopic3.getDestinationName() + "-noOps",
					new DestinationTopic.Properties(secondRetryProps3, "-noOps", DestinationTopic.Type.NO_OPS));

	protected List<DestinationTopic> allThirdDestinationTopics = Arrays
			.asList(mainDestinationTopic2, firstRetryDestinationTopic2, secondRetryDestinationTopic2, dltDestinationTopic2);

	// Classifiers

	private final BinaryExceptionClassifier classifier = new BinaryExceptionClassifierBuilder()
			.retryOn(IllegalArgumentException.class).build();

	private BiPredicate<Integer, Throwable> getShouldRetryOn() {
		return (a, e) -> a < maxAttempts && classifier.classify(e);
	}

	class PropsHolder {
		final String topicName;
		final DestinationTopic.Properties props;

		PropsHolder(String topicName, DestinationTopic.Properties props) {
			this.topicName = topicName;
			this.props = props;
		}
	}
}
