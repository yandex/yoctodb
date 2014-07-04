/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.mutable.segment;

import net.jcip.annotations.NotThreadSafe;

/**
 * Something freezable
 *
 * @author incubos
 */
@NotThreadSafe
public abstract class Freezable {
    private boolean frozen = false;

    protected void freeze() {
        frozen = true;
    }

    protected void checkNotFrozen() {
        if (frozen) {
            throw new IllegalStateException("Already frozen");
        }
    }
}
