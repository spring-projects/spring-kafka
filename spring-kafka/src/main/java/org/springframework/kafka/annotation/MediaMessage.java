package org.springframework.kafka.annotation;

import org.springframework.util.MimeTypeUtils;

import java.lang.annotation.*;

/**
 * Used to define the content type of {@code Message} with {@code MediaMessageResolver}.
 *
 * @author Scruel Tao
 * @date 2021/12/24
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface MediaMessage {
	/**
	 * The string value of the content type of message, which can support parse,
	 * Default application/json.
	 *
	 * @return the string value of mime type.
	 * @see org.springframework.util.MimeType
	 */
	String type() default MimeTypeUtils.APPLICATION_JSON_VALUE;

	/**
	 * Regardless of whether there is a type header in the {@code Message}, the parse
	 * progress will set the header of {@code Message} by this annotation {@link #type()}.
	 *
	 * @return whether to force parse with the annotation {@link #type()}
	 */
	boolean force() default false;
}
