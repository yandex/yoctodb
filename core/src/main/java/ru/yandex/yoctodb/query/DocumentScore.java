/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.Immutable;

/**
 * Comparable document score
 *
 * @author incubos
 */
@Immutable
public interface DocumentScore<T extends DocumentScore<T>>
        extends Comparable<T> {
}
