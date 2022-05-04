/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;

/**
 * Enables the non-blocking topic-based delayed retries feature. To be used in
 * {@link Configuration Configuration} classes as follows:
 * <pre class="code">
 *
 * &#064;EnableKafkaRetryTopic
 * &#064;Configuration
 * public class AppConfig {
 * }
 *
 * &#064;Component
 * public class MyListener {
 *
 *     &#064;RetryableTopic(fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC, backoff = @Backoff(4000))
 *     &#064;KafkaListener(topics =  "myTopic")
 * 	   public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
 *	       logger.info("Message {} received in topic {} ", message, receivedTopic);
 *     }
 *
 *     &#064;DltHandler
 *     public void dltHandler(Object message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
 *	       logger.info("Message {} received in dlt handler at topic {} ", message, receivedTopic);
 *     }
 * </pre>
 *
 * Using this annotation configures the default {@link RetryTopicConfigurationSupport}
 * bean. This annotation is meta-annotated with {@code @EnableKafka} so it is not
 * necessary to specify both.
 *
 * To configure the feature's components, extend the
 * {@link RetryTopicConfigurationSupport} class and override the appropriate methods on a
 * {@link Configuration @Configuration} class, such as:
 *
 * <pre class="code">
 *
 * &#064;Configuration
 * &#064;EnableKafka
 * public class AppConfig extends RetryTopicConfigurationSupport {
 * 		&#064;Override
 * 		protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
 * 			blockingRetries
 * 				.retryOn(ShouldRetryOnlyBlockingException.class, ShouldRetryViaBothException.class)
 * 				.backOff(new FixedBackOff(50, 3));
 * 		}
 *
 * 		&#064;Override
 * 		protected void configureNonBlockingRetries(NonBlockingRetriesConfigurer nonBlockingRetries) {
 * 			nonBlockingRetries
 * 				.addToFatalExceptions(ShouldSkipBothRetriesException.class);
 * }
 * </pre>
 * In this case, you should not use this annotation, use {@code @EnableKafka} instead.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import(RetryTopicConfigurationSupport.class)
@EnableKafka
public @interface EnableKafkaRetryTopic {
}
