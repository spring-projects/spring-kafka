/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import org.springframework.core.MethodParameter;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.MapAccessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.TypeUtils;

/**
 * An abstract {@link MessageListener} adapter
 * providing the necessary infrastructure to extract the payload of a
 * {@link Message}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Venil Noronha
 * @author Nathan Xu
 * @author Wang ZhiYang
 * @author Huijin Hong
 * @author Soby Chacko
 * @author Sanghyeok An
 */
public abstract class MessagingMessageListenerAdapter<K, V> implements ConsumerSeekAware, AsyncRepliesAware {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private static final Acknowledgment NO_OP_ACK = new NoOpAck();

	/**
	 * Message used when no conversion is needed.
	 */
	protected static final Message<KafkaNull> NULL_MESSAGE = new GenericMessage<>(KafkaNull.INSTANCE); // NOSONAR

	private static final boolean monoPresent =
			ClassUtils.isPresent("reactor.core.publisher.Mono", MessageListener.class.getClassLoader());

	private final @Nullable Object bean;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); //NOSONAR

	private final @Nullable Type inferredType;

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	private final @Nullable KafkaListenerErrorHandler errorHandler;

	@Nullable
	private HandlerAdapter handlerMethod;

	private boolean isConsumerRecordList;

	private boolean isConsumerRecords;

	private boolean isMessageList;

	private boolean conversionNeeded = true;

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private boolean converterSet;

	private Type fallbackType = Object.class;

	private @Nullable Expression replyTopicExpression;

	@SuppressWarnings("rawtypes")
	private @Nullable KafkaTemplate replyTemplate;

	private boolean hasAckParameter;

	private boolean noOpAck;

	private boolean hasMetadataParameter;

	private boolean messageReturnType;

	private @Nullable ReplyHeadersConfigurer replyHeadersConfigurer;

	private boolean splitIterables = true;

	private String correlationHeaderName = KafkaHeaders.CORRELATION_ID;

	private ObservationRegistry observationRegistry = ObservationRegistry.NOOP;

	private @Nullable BiConsumer<ConsumerRecord<K, V>, RuntimeException> asyncRetryCallback;

	/**
	 * Create an instance with the provided bean and method.
	 * @param bean the bean.
	 * @param method the method.
	 */
	protected MessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method) {
		this(bean, method, null);
	}

	/**
	 * Create an instance with the provided bean, method and kafka listener error handler.
	 * @param bean the bean.
	 * @param method the method.
	 * @param errorHandler the kafka listener error handler.
	 */
	@SuppressWarnings("this-escape")
	protected MessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method,
			@Nullable KafkaListenerErrorHandler errorHandler) {
		this.bean = bean;
		this.inferredType = determineInferredType(method); // NOSONAR = intentionally not final
		this.errorHandler = errorHandler;
	}

	/**
	 * Set a custom header name for the correlation id. Default
	 * {@link KafkaHeaders#CORRELATION_ID}. This header will be echoed back in any reply
	 * message.
	 * @param correlationHeaderName the header name.
	 * @since 3.0
	 */
	public void setCorrelationHeaderName(String correlationHeaderName) {
		Assert.notNull(correlationHeaderName, "'correlationHeaderName' cannot be null");
		this.correlationHeaderName = correlationHeaderName;
	}

	/**
	 * Set the MessageConverter.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(RecordMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
		this.converterSet = true;
	}

	/**
	 * Return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link Message}.
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link Message}.
	 */
	protected final RecordMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Set the {@link SmartMessageConverter} to use with the default
	 * {@link MessagingMessageConverter}. Not allowed when a custom
	 * {@link #setMessageConverter(RecordMessageConverter) messageConverter} is provided.
	 * @param messageConverter the converter.
	 * @since 2.7.1
	 */
	public void setMessagingConverter(SmartMessageConverter messageConverter) {
		Assert.isTrue(!this.converterSet, "Cannot set the SmartMessageConverter when setting the messageConverter, "
				+ "add the SmartConverter to the message converter instead");
		((MessagingMessageConverter) this.messageConverter).setMessagingConverter(messageConverter);
	}

	/**
	 * Return the inferred type for conversion or, if null, the
	 * {@link #setFallbackType(Class) fallbackType}.
	 * @return the type.
	 */
	protected Type getType() {
		return this.inferredType == null ? this.fallbackType : this.inferredType;
	}

	/**
	 * Set a fallback type to use when using a type-aware message converter and this
	 * adapter cannot determine the inferred type from the method. An example of a
	 * type-aware message converter is the {@code StringJsonMessageConverter}. Defaults to
	 * {@link Object}.
	 * @param fallbackType the type.
	 */
	public void setFallbackType(Class<?> fallbackType) {
		this.fallbackType = fallbackType;
	}

	/**
	 * Set the {@link HandlerAdapter} to use to invoke the method
	 * processing an incoming {@link ConsumerRecord}.
	 * @param handlerMethod {@link HandlerAdapter} instance.
	 */
	public void setHandlerMethod(HandlerAdapter handlerMethod) {
		this.handlerMethod = handlerMethod;
	}

	/**
	 * Set the {@link ObservationRegistry} to handle observability.
	 * @param observationRegistry {@link ObservationRegistry} instance.
	 * @since 3.3.0
	 */
	public void setObservationRegistry(ObservationRegistry observationRegistry) {
		this.observationRegistry = observationRegistry;
	}

	public boolean isAsyncReplies() {
		return this.handlerMethod != null && this.handlerMethod.isAsyncReplies();
	}

	protected boolean isConsumerRecordList() {
		return this.isConsumerRecordList;
	}

	public boolean isConsumerRecords() {
		return this.isConsumerRecords;
	}

	public boolean isConversionNeeded() {
		return this.conversionNeeded;
	}

	/**
	 * Set the topic to which to send any result from the method invocation.
	 * Maybe a SpEL expression {@code !{...}} evaluated at runtime.
	 * @param replyTopicParam the topic or expression.
	 * @since 2.0
	 */
	public void setReplyTopic(String replyTopicParam) {
		String replyTopic = replyTopicParam;
		if (!StringUtils.hasText(replyTopic)) {
			replyTopic = AdapterUtils.getDefaultReplyTopicExpression();
		}
		if (replyTopic.contains(AdapterUtils.PARSER_CONTEXT.getExpressionPrefix())) {
			this.replyTopicExpression = PARSER.parseExpression(replyTopic, AdapterUtils.PARSER_CONTEXT);
		}
		else {
			this.replyTopicExpression = new LiteralExpression(replyTopic);
		}

	}

	/**
	 * Set the template to use to send any result from the method invocation.
	 * @param replyTemplate the template.
	 * @since 2.0
	 */
	public void setReplyTemplate(KafkaTemplate<?, ?> replyTemplate) {
		this.replyTemplate = replyTemplate;
	}

	/**
	 * Set a bean resolver for runtime SpEL expressions. Also configures the evaluation
	 * context with a standard type converter and map accessor.
	 * @param beanResolver the resolver.
	 * @since 2.0
	 */
	public void setBeanResolver(BeanResolver beanResolver) {
		this.evaluationContext.setBeanResolver(beanResolver);
		this.evaluationContext.setTypeConverter(new StandardTypeConverter());
		this.evaluationContext.addPropertyAccessor(new MapAccessor());
	}

	/**
	 * Set the retry callback for failures of both {@link CompletableFuture} and {@link Mono}.
	 * {@link MessagingMessageListenerAdapter#asyncFailure(Object, Acknowledgment, Consumer, Throwable, Message)}
	 * will invoke {@link MessagingMessageListenerAdapter#asyncRetryCallback} when
	 * {@link CompletableFuture} or {@link Mono} fails to complete.
	 * @param asyncRetryCallback the callback for async retry.
	 * @since 3.3
	 */
	public void setCallbackForAsyncFailure(
			@Nullable BiConsumer<ConsumerRecord<K, V>, RuntimeException> asyncRetryCallback) {

		this.asyncRetryCallback = asyncRetryCallback;
	}

	protected boolean isMessageList() {
		return this.isMessageList;
	}

	/**
	 * Return the reply configurer.
	 * @return the configurer.
	 * @since 2.2
	 * @see #setReplyHeadersConfigurer(ReplyHeadersConfigurer)
	 */
	protected @Nullable ReplyHeadersConfigurer getReplyHeadersConfigurer() {
		return this.replyHeadersConfigurer;
	}

	/**
	 * Set a configurer which will be invoked when creating a reply message.
	 * @param replyHeadersConfigurer the configurer.
	 * @since 2.2
	 */
	public void setReplyHeadersConfigurer(ReplyHeadersConfigurer replyHeadersConfigurer) {
		this.replyHeadersConfigurer = replyHeadersConfigurer;
	}

	/**
	 * When true, {@link Iterable} return results will be split into discrete records.
	 * @return true to split.
	 * @since 2.3.5
	 */
	protected boolean isSplitIterables() {
		return this.splitIterables;
	}

	/**
	 * Set to {@code false} to disable splitting {@link Iterable} reply values into separate
	 * records.
	 * @param splitIterables false to disable; default true.
	 * @since 2.3.5
	 */
	public void setSplitIterables(boolean splitIterables) {
		this.splitIterables = splitIterables;
	}

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.registerSeekCallback(callback);
		}
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.onPartitionsAssigned(assignments, callback);
		}
	}

	@Override
	public void onPartitionsRevoked(@Nullable Collection<TopicPartition> partitions) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.onPartitionsRevoked(partitions);
		}
	}

	@Override
	public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.onIdleContainer(assignments, callback);
		}
	}

	protected Message<?> toMessagingMessage(ConsumerRecord<K, V> cRecord, @Nullable Object acknowledgment,
			@Nullable Object consumer) {

		return getMessageConverter().toMessage(cRecord, acknowledgment, consumer, getType());
	}

	protected void invoke(Object records, @Nullable Object acknowledgment, @Nullable Object consumer,
			final Message<?> message) {

		Throwable listenerError = null;
		Object result = null;
		Observation currentObservation = getCurrentObservation();
		try {
			result = invokeHandler(records, acknowledgment, message, consumer);
			// For share consumers, we don't handle results yet (TODO: Handle results with queues)
			// For regular consumers, handle results regardless of acknowledgment type/null status
			if (result != null && !(consumer instanceof ShareConsumer)) {
				handleResult(result, records, (Acknowledgment) acknowledgment, (Consumer<?, ?>) consumer, message);
			}
		}
		catch (ListenerExecutionFailedException e) {
			listenerError = e;
			currentObservation.error(e.getCause() != null ? e.getCause() : e);
			// For share consumers, throw the error back to the container
			if (consumer instanceof ShareConsumer) {
				throw e;
			}
			else {
				handleException(records, (Acknowledgment) acknowledgment, (Consumer<?, ?>) consumer, message, e);
			}
		}
		catch (Error e) {
			listenerError = e;
			currentObservation.error(e);
			throw e;
		}
		finally {
			if (listenerError != null || result == null) {
				currentObservation.stop();
			}
		}
	}

	private Observation getCurrentObservation() {
		Observation currentObservation = this.observationRegistry.getCurrentObservation();
		return currentObservation == null ? Observation.NOOP : currentObservation;
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 * @param data the data to process during invocation.
	 * @param acknowledgment the acknowledgment to use if any.
	 * @param message the message to process.
	 * @param consumer the consumer (can be Consumer or ShareConsumer).
	 * @return the result of invocation.
	 */
	@Nullable
	protected final Object invokeHandler(Object data, @Nullable Object acknowledgment, Message<?> message,
			@Nullable Object consumer) {

		Object ack = acknowledgment;
		// For regular Acknowledgment, check if we need to use NO_OP_ACK
		if (ack == null && this.noOpAck && !(consumer instanceof ShareConsumer)) {
			ack = NO_OP_ACK;
		}
		Assert.notNull(this.handlerMethod, "the 'handlerMethod' must not be null");
		try {
			if (data instanceof List && !this.isConsumerRecordList) {
				return this.handlerMethod.invoke(message, ack, consumer);
			}
			else if (this.hasMetadataParameter) {
				return this.handlerMethod.invoke(message, data, ack, consumer,
						AdapterUtils.buildConsumerRecordMetadata(data));
			}
			else {
				return this.handlerMethod.invoke(message, data, ack, consumer);
			}
		}
		catch (MessageConversionException ex) {
			if (ack instanceof ShareAcknowledgment) {
				throw checkAckArg((ShareAcknowledgment) ack, message, new MessageConversionException("Cannot handle message", ex));
			}
			else {
				throw checkAckArg(ack, message, new MessageConversionException("Cannot handle message", ex));
			}
		}
		catch (MethodArgumentNotValidException ex) {
			if (ack instanceof ShareAcknowledgment) {
				throw checkAckArg((ShareAcknowledgment) ack, message, ex);
			}
			else {
				throw checkAckArg(ack, message, ex);
			}
		}
		catch (MessagingException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()), ex);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
					this.handlerMethod.getMethodAsString(message.getPayload()) + "' threw exception", ex);
		}
	}

	private RuntimeException checkAckArg(@Nullable Object acknowledgment, Message<?> message, Exception ex) {
		if (this.hasAckParameter && acknowledgment == null) {
			return new ListenerExecutionFailedException("invokeHandler Failed",
					new IllegalStateException("No Acknowledgment available as an argument, " +
							"the listener container must have a MANUAL AckMode to populate the Acknowledgment."));
		}
		return new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
				"be invoked with the incoming message", message.getPayload()), ex);
	}

	private RuntimeException checkAckArg(@Nullable ShareAcknowledgment acknowledgment, Message<?> message, Exception ex) {
		if (this.hasAckParameter && acknowledgment == null) {
			return new ListenerExecutionFailedException("invokeHandler Failed",
					new IllegalStateException("No ShareAcknowledgment available as an argument, " +
							"the listener container must have an explicit acknowledgement mode to populate the Acknowledgment."));
		}
		return new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
				"be invoked with the incoming message", message.getPayload()), ex);
	}

	/**
	 * Handle the given result object returned from the listener method, sending a
	 * response message to the SendTo topic.
	 * @param resultArg the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param acknowledgment the acknowledgment to manual ack
	 * @param consumer the consumer to handler error
	 * @param source the source data for the method invocation - e.g.
	 * {@code o.s.messaging.Message<?>}; may be null
	 */
	@SuppressWarnings("try")
	protected void handleResult(Object resultArg, Object request, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer, @Nullable Message<?> source) {
		final Observation observation = getCurrentObservation();
		this.logger.debug(() -> "Listener method returned result [" + resultArg
				+ "] - generating response message for it");
		String replyTopic = evaluateReplyTopic(request, source, resultArg);
		Assert.state(replyTopic == null || this.replyTemplate != null,
				"a KafkaTemplate is required to support replies");

		Object result = resultArg instanceof InvocationResult invocationResult ?
				invocationResult.result() :
				resultArg;
		boolean messageReturnType = resultArg instanceof InvocationResult invocationResult ?
				invocationResult.messageReturnType() :
				this.messageReturnType;

		CompletableFuture<?> completableFutureResult;

		if (monoPresent && result instanceof Mono<?> mono) {
			if (acknowledgment == null || !acknowledgment.isOutOfOrderCommit()) {
				this.logger.warn("Container 'Acknowledgment' must be async ack for Mono<?> return type " +
						"(or Kotlin suspend function); otherwise the container will ack the message immediately");
			}
			completableFutureResult = mono.toFuture();
		}
		else if (!(result instanceof CompletableFuture<?>)) {
			completableFutureResult = CompletableFuture.completedFuture(result);
		}
		else {
			completableFutureResult = (CompletableFuture<?>) result;
			if (acknowledgment == null || !acknowledgment.isOutOfOrderCommit()) {
				this.logger.warn("Container 'Acknowledgment' must be async ack for Future<?> return type; "
						+ "otherwise the container will ack the message immediately");
			}
		}

		completableFutureResult.whenComplete((r, t) -> {
			try (var ignored = observation.openScope()) {
				if (t == null) {
					asyncSuccess(r, replyTopic, source, messageReturnType);
					if (isAsyncReplies()) {
						acknowledge(acknowledgment);
					}
				}
				else {
					Throwable cause = t instanceof CompletionException ? t.getCause() : t;
					cause = cause == null ? t : cause;
					observation.error(cause);
					asyncFailure(request, acknowledgment, consumer, cause, source);
				}
			}
			finally {
				observation.stop();
			}
		});
	}

	@Nullable
	private String evaluateReplyTopic(Object request, @Nullable Object source, Object result) {
		String replyTo = null;
		if (result instanceof InvocationResult invResult) {
			replyTo = evaluateTopic(request, source, result, invResult.sendTo());
		}
		else if (this.replyTopicExpression != null) {
			replyTo = evaluateTopic(request, source, result, this.replyTopicExpression);
		}
		return replyTo;
	}

	@Nullable
	private String evaluateTopic(Object request, @Nullable Object source, Object result, @Nullable Expression sendTo) {
		if (sendTo instanceof LiteralExpression) {
			return sendTo.getValue(String.class);
		}
		else {
			Object value = sendTo == null ? null
					: sendTo.getValue(this.evaluationContext, new ReplyExpressionRoot(request, source, result));
			boolean isByteArray = value instanceof byte[];
			if (!(value == null || value instanceof String || isByteArray)) {
				throw new IllegalStateException(
						"replyTopic expression must evaluate to a String or byte[], it is: "
								+ value.getClass().getName());
			}
			if (isByteArray) {
				return new String((byte[]) value, StandardCharsets.UTF_8); // NOSONAR
			}
			return (String) value;
		}
	}

	/**
	 * Send the result to the topic.
	 *
	 * @param result the result.
	 * @param topic the topic.
	 * @param source the source (input).
	 * @param returnTypeMessage true if we are returning message(s).
	 * @since 2.1.3
	 */
	@SuppressWarnings("unchecked")
	protected void sendResponse(Object result, @Nullable String topic, @Nullable Object source,
			boolean returnTypeMessage) {

		if (!returnTypeMessage && topic == null) {
			this.logger.debug(() -> "No replyTopic to handle the reply: " + result);
		}
		else if (result instanceof Message<?> mResult) {
			Message<?> reply = checkHeaders(mResult, topic, source);
			Objects.requireNonNull(this.replyTemplate).send(reply);
		}
		else if (result instanceof Iterable<?> iterable && (iterableOfMessages(iterable) || this.splitIterables)) {
			iterable.forEach(v -> {
				if (v instanceof Message<?> mv) {
					Message<?> aReply = checkHeaders(mv, topic, source);
					Objects.requireNonNull(this.replyTemplate).send(aReply);
				}
				else {
					Objects.requireNonNull(this.replyTemplate).send(Objects.requireNonNull(topic), v);
				}
			});
		}
		else {
			sendSingleResult(result, Objects.requireNonNull(topic), source);
		}
	}

	private boolean iterableOfMessages(Iterable<?> iterable) {
		Iterator<?> iterator = iterable.iterator();
		return iterator.hasNext() && iterator.next() instanceof Message;
	}

	private Message<?> checkHeaders(Message<?> reply, @Nullable String topic, @Nullable Object source) { // NOSONAR (complexity)
		MessageHeaders headers = reply.getHeaders();
		boolean needsTopic = topic != null && headers.get(KafkaHeaders.TOPIC) == null;
		boolean sourceIsMessage = source instanceof Message;
		boolean needsCorrelation = headers.get(this.correlationHeaderName) == null && sourceIsMessage
				&& getCorrelation(Objects.requireNonNull((Message<?>) source)) != null;
		boolean needsPartition = headers.get(KafkaHeaders.PARTITION) == null && sourceIsMessage
				&& getReplyPartition(Objects.requireNonNull((Message<?>) source)) != null;
		if (needsTopic || needsCorrelation || needsPartition) {
			MessageBuilder<?> builder = MessageBuilder.fromMessage(reply);
			if (needsTopic) {
				builder.setHeader(KafkaHeaders.TOPIC, topic);
			}
			if (needsCorrelation) {
				setCorrelation(builder, (Message<?>) source);
			}
			if (needsPartition) {
				setPartition(builder, (Message<?>) source);
			}
			reply = builder.build();
		}
		return reply;
	}

	@SuppressWarnings("unchecked")
	private void sendSingleResult(Object result, String topic, @Nullable Object source) {
		if (source instanceof Message<?> message) {
			sendReplyForMessageSource(result, topic, message, getCorrelation(message));
		}
		else {
			Objects.requireNonNull(this.replyTemplate).send(topic, result);
		}
	}

	@SuppressWarnings("unchecked")
	private void sendReplyForMessageSource(Object result, String topic, Message<?> source, byte[] correlationId) {
		MessageBuilder<Object> builder = MessageBuilder.withPayload(result)
				.setHeader(KafkaHeaders.TOPIC, topic);
		if (this.replyHeadersConfigurer != null) {
			Map<String, Object> headersToCopy = source.getHeaders().entrySet().stream()
					.filter(e -> {
						String key = e.getKey();
						return !key.equals(MessageHeaders.ID) && !key.equals(MessageHeaders.TIMESTAMP)
								&& !key.equals(this.correlationHeaderName)
								&& !key.startsWith(KafkaHeaders.RECEIVED);
					})
					.filter(e -> this.replyHeadersConfigurer.shouldCopy(e.getKey(), e.getValue()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			if (!headersToCopy.isEmpty()) {
				builder.copyHeaders(headersToCopy);
			}
			headersToCopy = this.replyHeadersConfigurer.additionalHeaders();
			if (!ObjectUtils.isEmpty(headersToCopy)) {
				builder.copyHeaders(headersToCopy);
			}
		}
		if (correlationId != null) {
			builder.setHeader(this.correlationHeaderName, correlationId);
		}
		setPartition(builder, source);
		setKey(builder, source);
		Objects.requireNonNull(this.replyTemplate).send(builder.build());
	}

	protected void asyncSuccess(@Nullable Object result, @Nullable String replyTopic, @Nullable Message<?> source,
			boolean returnTypeMessage) {

		if (result == null) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Async result is null, ignoring");
			}
		}
		else {
			sendResponse(result, replyTopic, source, returnTypeMessage);
		}
	}

	protected void acknowledge(@Nullable Acknowledgment acknowledgment) {
		if (acknowledgment != null) {
			acknowledgment.acknowledge();
		}
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	protected void asyncFailure(Object request, @Nullable Acknowledgment acknowledgment, @Nullable Consumer<?, ?> consumer,
			@Nullable Throwable t, @Nullable Message<?> source) {

		try {
			Throwable cause = t instanceof CompletionException ? t.getCause() : t;
			handleException(request, acknowledgment, consumer, source,
					new ListenerExecutionFailedException(createMessagingErrorMessage(
							"Async Fail", Objects.requireNonNull(source).getPayload()), cause));
		}
		catch (Throwable ex) {
			acknowledge(acknowledgment);
			if (canAsyncRetry(request, ex) && this.asyncRetryCallback != null) {
				@SuppressWarnings("unchecked")
				ConsumerRecord<K, V> record = (ConsumerRecord<K, V>) request;
				this.asyncRetryCallback.accept(record, (RuntimeException) ex);
			}
			else {
				this.logger.error(ex, () -> "Future, Mono, or suspend function was completed with an exception for " + source);
			}
		}
	}

	private static boolean canAsyncRetry(Object request, Throwable exception) {
		// The async retry with @RetryableTopic is only supported for SingleRecord Listener.
		return request instanceof ConsumerRecord && exception instanceof RuntimeException;
	}

	protected void handleException(Object records, @Nullable Acknowledgment acknowledgment, @Nullable Consumer<?, ?> consumer,
			@Nullable Message<?> message, ListenerExecutionFailedException e) {

		if (this.errorHandler != null) {
			try {
				if (NULL_MESSAGE.equals(message)) {
					message = new GenericMessage<>(records);
				}
				Object errorResult = this.errorHandler.handleError(Objects.requireNonNull(message), e, consumer, acknowledgment);
				if (errorResult != null && !(errorResult instanceof InvocationResult) && this.handlerMethod != null) {
					Object result = this.handlerMethod.getInvocationResultFor(errorResult, message.getPayload());
					handleResult(Objects.requireNonNullElse(result, errorResult),
							records, acknowledgment, consumer, message);
				}
			}
			catch (Exception ex) {
				throw new ListenerExecutionFailedException(createMessagingErrorMessage(// NOSONAR stack trace loss
						"Listener error handler threw an exception for the incoming message",
						Objects.requireNonNull(message).getPayload()), ex);
			}
		}
		else {
			throw e;
		}
	}

	private void setCorrelation(MessageBuilder<?> builder, @Nullable Message<?> source) {
		byte[] correlationBytes = getCorrelation(source);
		if (correlationBytes != null) {
			builder.setHeader(this.correlationHeaderName, correlationBytes);
		}
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private byte[] getCorrelation(@Nullable Message<?> source) {
		return source.getHeaders().get(this.correlationHeaderName, byte[].class);
	}

	private void setPartition(MessageBuilder<?> builder, @Nullable Message<?> source) {
		byte[] partitionBytes = getReplyPartition(source);
		if (partitionBytes != null) {
			builder.setHeader(KafkaHeaders.PARTITION, ByteBuffer.wrap(partitionBytes).getInt());
		}
	}

	private void setKey(MessageBuilder<?> builder, @Nullable Message<?> source) {
		Object key = Objects.requireNonNull(source).getHeaders().get(KafkaHeaders.RECEIVED_KEY);
		// Set the reply record key only for non-batch requests
		if (key != null && !(key instanceof List)) {
			builder.setHeader(KafkaHeaders.KEY, key);
		}
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	private byte[] getReplyPartition(@Nullable Message<?> source) {
		return source.getHeaders().get(KafkaHeaders.REPLY_PARTITION, byte[].class);
	}

	protected final String createMessagingErrorMessage(String description, Object payload) {
		return description + "\n"
				+ "Endpoint handler details:\n"
				+ "Method [" + Objects.requireNonNull(this.handlerMethod).getMethodAsString(payload) + "]\n"
				+ "Bean [" + this.handlerMethod.getBean() + "]";
	}

	/**
	 * Subclasses can override this method to use a different mechanism to determine
	 * the target type of the payload conversion.
	 * @param method the method.
	 * @return the type.
	 */
	@Nullable
	protected Type determineInferredType(@Nullable Method method) { // NOSONAR complexity
		if (method == null) {
			return null;
		}

		Type genericParameterType = null;
		int allowedBatchParameters = 1;
		int notConvertibleParameters = 0;

		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			/*
			 * We're looking for a single non-annotated parameter, or one annotated with @Payload.
			 * We ignore parameters with type Message, Consumer, Ack, ConsumerRecord because they
			 * are not involved with conversion.
			 */
			Type parameterType = methodParameter.getGenericParameterType();
			boolean isNotConvertible = parameterIsType(parameterType, ConsumerRecord.class);
			boolean isAck = parameterIsType(parameterType, Acknowledgment.class);
			if (!isAck) {
				isAck = parameterIsType(parameterType, ShareAcknowledgment.class);
			}
			this.hasAckParameter |= isAck;
			if (isAck) {
				this.noOpAck |= methodParameter.getParameterAnnotation(NonNull.class) != null;
			}
			isNotConvertible |= isAck;
			boolean isConsumer = parameterIsType(parameterType, Consumer.class);
			isNotConvertible |= isConsumer;
			boolean isKotlinContinuation = AdapterUtils.isKotlinContinuation(methodParameter.getParameterType());
			isNotConvertible |= isKotlinContinuation;
			boolean isMeta = parameterIsType(parameterType, ConsumerRecordMetadata.class);
			this.hasMetadataParameter |= isMeta;
			isNotConvertible |= isMeta;
			if (isNotConvertible) {
				notConvertibleParameters++;
			}
			if (!isNotConvertible && !isMessageWithNoTypeInfo(parameterType)
					&& (methodParameter.getParameterAnnotations().length == 0
					|| methodParameter.hasParameterAnnotation(Payload.class))) {
				if (genericParameterType == null) {
					genericParameterType = extractGenericParameterTypFromMethodParameter(methodParameter);
				}
				else {
					if (methodParameter.hasParameterAnnotation(Payload.class)) {
						genericParameterType = parameterType;
					}
					else {
						this.logger.debug(() -> "Ambiguous parameters for target payload for method " + method
								+ "; no inferred type available");
					}
					break;
				}
			}
			else if (isAck || isKotlinContinuation || isConsumer || annotationHeaderIsGroupId(methodParameter)) {
				allowedBatchParameters++;
			}
		}

		if (notConvertibleParameters == method.getParameterCount() && method.getReturnType().equals(void.class)) {
			this.conversionNeeded = false;
		}
		boolean validParametersForBatch = method.getGenericParameterTypes().length <= allowedBatchParameters;
		if (!validParametersForBatch) {
			String stateMessage = "A parameter of type '%s' must be the only parameter "
					+ "(except for an optional 'Acknowledgment' and/or 'Consumer' "
					+ "and/or '@Header(KafkaHeaders.GROUP_ID) String groupId'";
			Assert.state(!this.isConsumerRecords,
					() -> String.format(stateMessage, "ConsumerRecords"));
			Assert.state(!this.isConsumerRecordList,
					() -> String.format(stateMessage, "List<ConsumerRecord>"));
			Assert.state(!this.isMessageList,
					() -> String.format(stateMessage, "List<Message<?>>"));
		}
		this.messageReturnType = KafkaUtils.returnTypeMessageOrCollectionOf(method);
		return genericParameterType;
	}

	private Type extractGenericParameterTypFromMethodParameter(MethodParameter methodParameter) {
		Type genericParameterType = methodParameter.getGenericParameterType();
		if (genericParameterType instanceof ParameterizedType parameterizedType) {
			Type rawType = parameterizedType.getRawType();
			if (rawType.equals(Message.class)) {
				genericParameterType = parameterizedType.getActualTypeArguments()[0];
			}
			else if (rawType.equals(List.class) && parameterizedType.getActualTypeArguments().length == 1) {
				Type paramType = parameterizedType.getActualTypeArguments()[0];
				boolean messageHasGeneric = paramType instanceof ParameterizedType pType
						&& pType.getRawType().equals(Message.class);
				this.isMessageList = TypeUtils.isAssignable(paramType, Message.class) || messageHasGeneric;
				this.isConsumerRecordList = TypeUtils.isAssignable(paramType, ConsumerRecord.class);
				if (messageHasGeneric) {
					genericParameterType = ((ParameterizedType) paramType).getActualTypeArguments()[0];
				}
			}
			else {
				this.isConsumerRecords = rawType.equals(ConsumerRecords.class);
			}
		}
		return genericParameterType;
	}

	private static boolean annotationHeaderIsGroupId(MethodParameter methodParameter) {
		Header header = methodParameter.getParameterAnnotation(Header.class);
		return header != null && KafkaHeaders.GROUP_ID.equals(header.value());
	}

	private static boolean isMessageWithNoTypeInfo(Type parameterType) {
		if (parameterType instanceof ParameterizedType pType && pType.getRawType().equals(Message.class)) {
			return pType.getActualTypeArguments()[0] instanceof WildcardType;
		}
		return Message.class.equals(parameterType); // could be Message without a generic type
	}

	private static boolean parameterIsType(Type parameterType, Type type) {
		return parameterType.equals(type) || rawByParameterIsType(parameterType, type);
	}

	private static boolean rawByParameterIsType(Type parameterType, Type type) {
		return parameterType instanceof ParameterizedType pType && pType.getRawType().equals(type);
	}

	/**
	 * Root object for reply expression evaluation.
	 * @param request the request.
	 * @param source the source.
	 * @param result the result.
	 * @since 2.0
	 */
	public record ReplyExpressionRoot(Object request, @Nullable Object source, Object result) {

	}

	static class NoOpAck implements Acknowledgment {

		@Override
		public void acknowledge() {
		}

	}

	static class NoOpShareAck implements ShareAcknowledgment {

		@Override
		public void acknowledge() {
		}

		@Override
		public void release() {
		}

		@Override
		public void reject() {
		}

	}

}
