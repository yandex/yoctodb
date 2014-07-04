/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
