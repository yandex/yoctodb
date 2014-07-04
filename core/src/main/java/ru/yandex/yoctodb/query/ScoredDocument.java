/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.Database;

/**
 * Scored document comparable by {@link DocumentScore}
 *
 * @author incubos
 */
@Immutable
public interface ScoredDocument<T extends ScoredDocument<T>>
        extends Comparable<T> {
    @NotNull
    Database getDatabase();

    int getDocument();
}
