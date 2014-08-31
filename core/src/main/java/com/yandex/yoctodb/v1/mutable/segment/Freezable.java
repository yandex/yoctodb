/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

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
