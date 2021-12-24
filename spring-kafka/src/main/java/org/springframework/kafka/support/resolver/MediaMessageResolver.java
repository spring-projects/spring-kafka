package org.springframework.kafka.support.resolver;

import org.springframework.core.MethodParameter;
import org.springframework.kafka.annotation.MediaMessage;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.support.MessageBuilder;

/**
 * A resolver to extract and convert the payload of a message using a MessageConverter.
 * This HandlerMethodArgumentResolver only work with MediaMessage annotation now.
 *
 * @author Scruel Tao
 */
public class MediaMessageResolver extends PayloadMethodArgumentResolver {
	/**
	 * Create a new {@code MediaMessageResolver} with the given
	 * {@link MessageConverter}.
	 *
	 * @param messageConverter the MessageConverter to use (required)
	 */
	public MediaMessageResolver(MessageConverter messageConverter) {
		super(messageConverter);
	}

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		return parameter.hasParameterAnnotation(MediaMessage.class);
	}

	@Override
	public Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
		MediaMessage ann = parameter.getParameterAnnotation(MediaMessage.class);
		if (null == ann) {
			throw new IllegalStateException("Annotation parsing failed.");
		}
		// If not present, parse content type from annotation
		if (message.getHeaders().get(MessageHeaders.CONTENT_TYPE) == null) {
			message = MessageBuilder.fromMessage(message)
					.setHeader(MessageHeaders.CONTENT_TYPE, ann.type())
					.build();
		}
		return super.resolveArgument(parameter, message);
	}

}
