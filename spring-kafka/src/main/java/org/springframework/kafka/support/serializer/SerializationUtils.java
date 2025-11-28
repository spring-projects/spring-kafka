/*
 * Copyright 2020-present the original author or authors.
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

package org.springframework.kafka.support.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Utilities for serialization.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public final class SerializationUtils {

	/**
	 * Header name for deserialization exceptions.
	 * @since 2.8
	 * @deprecated for removal in favor of {@link KafkaUtils#DESERIALIZER_EXCEPTION_HEADER_PREFIX}
	 */
	@Deprecated(forRemoval = true)
	public static final String DESERIALIZER_EXCEPTION_HEADER_PREFIX = KafkaUtils.DESERIALIZER_EXCEPTION_HEADER_PREFIX;

	/**
	 * Header name for deserialization exceptions.
	 * @since 2.8
	 * @deprecated for removal in favor of {@link KafkaUtils#KEY_DESERIALIZER_EXCEPTION_HEADER}
	 */
	@Deprecated(forRemoval = true)
	public static final String KEY_DESERIALIZER_EXCEPTION_HEADER = KafkaUtils.KEY_DESERIALIZER_EXCEPTION_HEADER;

	/**
	 * Header name for deserialization exceptions.
	 * @since 2.8
	 * @deprecated for removal in favor of {@link KafkaUtils#VALUE_DESERIALIZER_EXCEPTION_HEADER}
	 */
	@Deprecated(forRemoval = true)
	public static final String VALUE_DESERIALIZER_EXCEPTION_HEADER = KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER;

	private SerializationUtils() {
	}

	/**
	 * Convert a property value (FQCN.methodName) to a {@link BiFunction} that takes a
	 * payload and headers and returns some value. The method must have parameters
	 * {@code (P, Headers)} or {@code (P)} and be declared as static.
	 * @param <P> The {@link BiFunction} first parameter type.
	 * @param <T> The {@link BiFunction} return type.
	 * @param methodProperty the method name property.
	 * @param payloadType the {@link BiFunction} first parameter type.
	 * @param classLoader the class loader.
	 * @return the function.
	 */
	@SuppressWarnings("unchecked")
	public static <P, T> BiFunction<P, Headers, T> propertyToMethodInvokingFunction(String methodProperty,
			Class<P> payloadType, ClassLoader classLoader) {

		int lastDotPosn = methodProperty.lastIndexOf('.');
		Assert.state(lastDotPosn > 1,
				"the method property needs to be a class name followed by the method name, separated by '.'");
		BiFunction<P, Headers, T> function;
		Class<?> clazz;
		try {
			clazz = ClassUtils.forName(methodProperty.substring(0, lastDotPosn), classLoader);
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalStateException(e);
		}
		String methodName = methodProperty.substring(lastDotPosn + 1);
		Method method;
		try {
			method = clazz.getDeclaredMethod(methodName, payloadType, Headers.class);
		}
		catch (@SuppressWarnings("unused") NoSuchMethodException e) {
			try {
				method = clazz.getDeclaredMethod(methodName, payloadType);
			}
			catch (@SuppressWarnings("unused") NoSuchMethodException e1) {
				IllegalStateException ise =
						new IllegalStateException("the parser method must take '("
								+ payloadType.getSimpleName()
								+ ", Headers)' or '("
								+ payloadType.getSimpleName()
								+ ")'", e1);
				ise.addSuppressed(e);
				throw ise; // NOSONAR, lost stack trace
			}
			catch (SecurityException e1) {
				IllegalStateException ise = new IllegalStateException(e1);
				ise.addSuppressed(e);
				throw ise; // NOSONAR, lost stack trace
			}
		}
		catch (SecurityException e) {
			throw new IllegalStateException(e);
		}
		Method parseMethod = method;
		if (method.getParameters().length > 1) {
			function = (str, headers) -> {
				try {
					return (T) parseMethod.invoke(null, str, headers);
				}
				catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					throw new IllegalStateException(e);
				}
			};
		}
		else {
			function = (str, headers) -> {
				try {
					return (T) parseMethod.invoke(null, str);
				}
				catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					throw new IllegalStateException(e);
				}
			};
		}
		return function;
	}

	/**
	 * Populate the record headers with a serialized {@link DeserializationException}.
	 * @param headers the headers.
	 * @param data the data.
	 * @param ex the exception.
	 * @param isForKeyArg true if this is a key deserialization problem, otherwise value.
	 * @since 2.8
	 */
	public static void deserializationException(Headers headers, byte[] data, Exception ex, boolean isForKeyArg) {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		DeserializationException exception =
				new DeserializationException("failed to deserialize", data, isForKeyArg, ex);
		try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
			oos.writeObject(exception);
		}
		catch (IOException ioex) {
			stream = new ByteArrayOutputStream();
			try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
				exception = new DeserializationException("failed to deserialize",
						data, isForKeyArg, new RuntimeException("Could not serialize type "
						+ ex.getClass().getName() + " with message " + ioex.getMessage()
						+ ". Original exception message: " + ex.getMessage()));
				oos.writeObject(exception);
			}
			catch (IOException ex2) {
				throw new IllegalStateException("Could not serialize a DeserializationException", ex2); // NOSONAR
			}
		}
		headers.add(
				new DeserializationExceptionHeader(isForKeyArg
						? KafkaUtils.KEY_DESERIALIZER_EXCEPTION_HEADER
						: KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
						stream.toByteArray()));
	}

	/**
	 * Extract a {@link DeserializationException} from the supplied header name, if
	 * present.
	 * @param record the consumer record.
	 * @param headerName the header name.
	 * @param logger the logger for logging errors.
	 * @return the exception or null.
	 * @since 2.9.11
	 */
	@Nullable
	public static DeserializationException getExceptionFromHeader(final ConsumerRecord<?, ?> record,
			String headerName, LogAccessor logger) {

		Header header = record.headers().lastHeader(headerName);
		if (header != null && !(header instanceof DeserializationExceptionHeader)) {
			logger.warn(
					() -> String.format("Foreign deserialization exception header in (%s) ignored; possible attack?",
							KafkaUtils.format(record)));
			return null;
		}
		if (header != null) {
			DeserializationException exception = byteArrayToDeserializationException(logger, header);
			if (exception != null) {
				Headers headers = new RecordHeaders(record.headers().toArray());
				headers.remove(headerName);
				exception.setHeaders(headers);
			}
			return exception;
		}
		return null;
	}

	/**
	 * Convert a byte array containing a serialized {@link DeserializationException} to the
	 * {@link DeserializationException}.
	 * @param logger a log accessor to log errors.
	 * @param header the header.
	 * @return the exception or null if deserialization fails.
	 * @since 2.9.11
	 */
	@Nullable
	public static DeserializationException byteArrayToDeserializationException(LogAccessor logger, Header header) {

		if (header != null && !(header instanceof DeserializationExceptionHeader)) {
			throw new IllegalStateException("Foreign deserialization exception header ignored; possible attack?");
		}
		try {
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(header.value())) {

				boolean first = true;

				@Override
				protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
					if (this.first) {
						this.first = false;
						Assert.state(desc.getName().equals(DeserializationException.class.getName()),
								"Header does not contain a DeserializationException");
					}
					return super.resolveClass(desc);
				}

			};
			return (DeserializationException) ois.readObject();
		}
		catch (IOException | ClassNotFoundException | ClassCastException e) {
			logger.error(e, "Failed to deserialize a deserialization exception");
			return null;
		}
	}

}
