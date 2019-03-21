/*
 * Copyright 2016-2017 the original author or authors.
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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.expression.MapAccessor;
import org.springframework.core.MethodParameter;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.Assert;

/**
 * An abstract {@link MessageListener} adapter providing the necessary infrastructure
 * to extract the payload of a {@link org.springframework.messaging.Message}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Venil Noronha
 */
public abstract class MessagingMessageListenerAdapter<K, V> implements ConsumerSeekAware {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

	private final Object bean;

	protected final Log logger = LogFactory.getLog(getClass()); //NOSONAR

	private final Type inferredType;

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	private HandlerAdapter handlerMethod;

	private boolean isConsumerRecordList;

	private boolean isMessageList;

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private Type fallbackType = Object.class;

	private Expression replyTopicExpression;

	private KafkaTemplate<K, V> replyTemplate;

	private boolean hasAckParameter;

	public MessagingMessageListenerAdapter(Object bean, Method method) {
		this.bean = bean;
		this.inferredType = determineInferredType(method);
	}

	/**
	 * Set the MessageConverter.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(RecordMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final RecordMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Returns the inferred type for conversion or, if null, the
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

	protected boolean isConsumerRecordList() {
		return this.isConsumerRecordList;
	}

	/**
	 * Set the topic to which to send any result from the method invocation.
	 * May be a SpEL expression {@code !{...}} evaluated at runtime.
	 * @param replyTopic the topic or expression.
	 * @since 2.0
	 */
	public void setReplyTopic(String replyTopic) {
		if (replyTopic.startsWith(PARSER_CONTEXT.getExpressionPrefix())) {
			this.replyTopicExpression = PARSER.parseExpression(replyTopic, PARSER_CONTEXT);
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
	public void setReplyTemplate(KafkaTemplate<K, V> replyTemplate) {
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

	protected boolean isMessageList() {
		return this.isMessageList;
	}

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware) {
			((ConsumerSeekAware) this.bean).registerSeekCallback(callback);
		}
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware) {
			((ConsumerSeekAware) this.bean).onPartitionsAssigned(assignments, callback);
		}
	}

	@Override
	public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware) {
			((ConsumerSeekAware) this.bean).onIdleContainer(assignments, callback);
		}
	}

	protected Message<?> toMessagingMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment,
			Consumer<?, ?> consumer) {
		return getMessageConverter().toMessage(record, acknowledgment, consumer, getType());
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 * @param data the data to process during invocation.
	 * @param acknowledgment the acknowledgment to use if any.
	 * @param message the message to process.
	 * @param consumer the consumer.
	 * @return the result of invocation.
	 */
	protected final Object invokeHandler(Object data, Acknowledgment acknowledgment, Message<?> message,
			Consumer<?, ?> consumer) {
		try {
			if (data instanceof List && !this.isConsumerRecordList) {
				return this.handlerMethod.invoke(message, acknowledgment, consumer);
			}
			else {
				return this.handlerMethod.invoke(message, data, acknowledgment, consumer);
			}
		}
		catch (org.springframework.messaging.converter.MessageConversionException ex) {
			if (this.hasAckParameter && acknowledgment == null) {
				throw new ListenerExecutionFailedException("invokeHandler Failed",
						new IllegalStateException("No Acknowledgment available as an argument, "
						+ "the listener container must have a MANUAL Ackmode to populate the Acknowledgment.", ex));
			}
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()),
					new MessageConversionException("Cannot handle message", ex));
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

	/**
	 * Handle the given result object returned from the listener method, sending a
	 * response message to the SendTo topic.
	 * @param resultArg the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param source the source data for the method invocation - e.g.
	 * {@code o.s.messaging.Message<?>}; may be null
	 */
	protected void handleResult(Object resultArg, Object request, Object source) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Listener method returned result [" + resultArg
					+ "] - generating response message for it");
		}
		Object result = resultArg instanceof ResultHolder ? ((ResultHolder) resultArg).result : resultArg;
		String replyTopic = evaluateReplyTopic(request, source, resultArg);
		Assert.state(replyTopic == null || this.replyTemplate != null,
				"a KafkaTemplate is required to support replies");
		sendResponse(result, replyTopic);
	}

	private String evaluateReplyTopic(Object request, Object source, Object result) {
		String replyTo = null;
		if (result instanceof ResultHolder) {
			replyTo = evaluateTopic(request, source, result, ((ResultHolder) result).sendTo);
		}
		else if (this.replyTopicExpression != null) {
			replyTo = evaluateTopic(request, source, result, this.replyTopicExpression);
		}
		return replyTo;
	}

	private String evaluateTopic(Object request, Object source, Object result, Expression sendTo) {
		if (sendTo instanceof LiteralExpression) {
			return sendTo.getValue(String.class);
		}
		else {
			Object value = sendTo.getValue(this.evaluationContext, new ReplyExpressionRoot(request, source, result));
			Assert.state(value instanceof String, "replyTopic expression must evaluate to a String or Address");
			return (String) value;
		}
	}

	@SuppressWarnings("unchecked")
	protected void sendResponse(Object result, String topic) {
		if (topic == null) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("No replyTopic to handle the reply: " + result);
			}
		}
		else {
			if (result instanceof Collection) {
				((Collection<V>) result).forEach(v -> {
					this.replyTemplate.send(topic, v);
				});
			}
			else {
				this.replyTemplate.send(topic, (V) result);
			}
		}
	}

	protected final String createMessagingErrorMessage(String description, Object payload) {
		return description + "\n"
				+ "Endpoint handler details:\n"
				+ "Method [" + this.handlerMethod.getMethodAsString(payload) + "]\n"
				+ "Bean [" + this.handlerMethod.getBean() + "]";
	}

	/**
	 * Subclasses can override this method to use a different mechanism to determine
	 * the target type of the payload conversion.
	 * @param method the method.
	 * @return the type.
	 */
	protected Type determineInferredType(Method method) {
		if (method == null) {
			return null;
		}

		Type genericParameterType = null;
		boolean hasConsumerParameter = false;

		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			/*
			 * We're looking for a single non-annotated parameter, or one annotated with @Payload.
			 * We ignore parameters with type Message because they are not involved with conversion.
			 */
			if (eligibleParameter(methodParameter)
					&& (methodParameter.getParameterAnnotations().length == 0
					|| methodParameter.hasParameterAnnotation(Payload.class))) {
				if (genericParameterType == null) {
					genericParameterType = methodParameter.getGenericParameterType();
					if (genericParameterType instanceof ParameterizedType) {
						ParameterizedType parameterizedType = (ParameterizedType) genericParameterType;
						if (parameterizedType.getRawType().equals(Message.class)) {
							genericParameterType = ((ParameterizedType) genericParameterType)
								.getActualTypeArguments()[0];
						}
						else if (parameterizedType.getRawType().equals(List.class)
								&& parameterizedType.getActualTypeArguments().length == 1) {
							Type paramType = parameterizedType.getActualTypeArguments()[0];
							this.isConsumerRecordList =	paramType.equals(ConsumerRecord.class)
									|| (paramType instanceof ParameterizedType
										&& ((ParameterizedType) paramType).getRawType().equals(ConsumerRecord.class));
							this.isMessageList = paramType.equals(Message.class)
									|| (paramType instanceof ParameterizedType
											&& ((ParameterizedType) paramType).getRawType().equals(Message.class));
						}
					}
				}
				else {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Ambiguous parameters for target payload for method " + method
										+ "; no inferred type available");
					}
					break;
				}
			}
			else if (methodParameter.getGenericParameterType().equals(Acknowledgment.class)) {
				this.hasAckParameter = true;
			}
			else {
				if (methodParameter.getGenericParameterType().equals(Consumer.class)) {
					hasConsumerParameter = true;
				}
				else {
					Type parameterType = methodParameter.getGenericParameterType();
					hasConsumerParameter = parameterType instanceof ParameterizedType
							&& ((ParameterizedType) parameterType).getRawType().equals(Consumer.class);
				}
			}
		}
		boolean validParametersForBatch = validParametersForBatch(method.getGenericParameterTypes().length,
				this.hasAckParameter, hasConsumerParameter);
		String stateMessage = "A parameter of type 'List<%s>' must be the only parameter "
				+ "(except for an optional 'Acknowledgment' and/or 'Consumer')";
		Assert.state(!this.isConsumerRecordList || validParametersForBatch,
				() -> String.format(stateMessage, "ConsumerRecord"));
		Assert.state(!this.isMessageList || validParametersForBatch,
				() -> String.format(stateMessage, "Message<?>"));
		return genericParameterType;
	}

	private boolean validParametersForBatch(int parameterCount, boolean hasAck, boolean hasConsumer) {
		if (hasAck) {
			return parameterCount == 2 || (hasConsumer && parameterCount == 3);
		}
		else if (hasConsumer) {
			return parameterCount == 2;
		}
		else {
			return parameterCount == 1;
		}
	}

	/*
	 * Don't consider parameter types that are available after conversion.
	 * Acknowledgment, ConsumerRecord, Consumer, ConsumerRecord<...>, Consumer<...>, and Message<?>.
	 */
	private boolean eligibleParameter(MethodParameter methodParameter) {
		Type parameterType = methodParameter.getGenericParameterType();
		if (parameterType.equals(Acknowledgment.class) || parameterType.equals(ConsumerRecord.class)
				|| parameterType.equals(Consumer.class)) {
			return false;
		}
		if (parameterType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) parameterType;
			Type rawType = parameterizedType.getRawType();
			if (rawType.equals(ConsumerRecord.class) || rawType.equals(Consumer.class)) {
				return false;
			}
			else if (rawType.equals(Message.class)) {
				return !(parameterizedType.getActualTypeArguments()[0] instanceof WildcardType);
			}
		}
		return !parameterType.equals(Message.class); // could be Message without a generic type
	}

	/**
	 * Result holder.
	 * @since 2.0
	 */
	public static final class ResultHolder {

		private final Object result;

		private final Expression sendTo;

		public ResultHolder(Object result, Expression sendTo) {
			this.result = result;
			this.sendTo = sendTo;
		}

		@Override
		public String toString() {
			return this.result.toString();
		}

	}

	/**
	 * Root object for reply expression evaluation.
	 * @since 2.0
	 */
	public static final class ReplyExpressionRoot {

		private final Object request;

		private final Object source;

		private final Object result;

		public ReplyExpressionRoot(Object request, Object source, Object result) {
			this.request = request;
			this.source = source;
			this.result = result;
		}

		public Object getRequest() {
			return this.request;
		}

		public Object getSource() {
			return this.source;
		}

		public Object getResult() {
			return this.result;
		}

	}

}
