/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.util.Collection;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Utility class that suffixes strings.
 *
 * @author Tomaz Fernandes
 * @author Ngoc Nhan
 * @since 2.7
 *
 */
public class Suffixer {

	private final String suffix;

	public Suffixer(String suffix) {
		Assert.notNull(suffix, "Suffix cannot be null");
		this.suffix = suffix;
	}

	public @Nullable String maybeAddTo(@Nullable String source) {
		if (!StringUtils.hasText(this.suffix)) {
			return source;
		}
		return StringUtils.hasText(source) // Only suffix if there's text
				? source.concat(this.suffix)
				: source;
	}

	public Collection<String> maybeAddTo(Collection<String> sources) {
		if (!StringUtils.hasText(this.suffix)) {
			return sources;
		}
		return sources
				.stream()
				.map(source -> maybeAddTo(source))
				.collect(Collectors.toList());
	}
}
