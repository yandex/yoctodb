/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
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

    public void freeze() {
        frozen = true;
    }

    public void checkNotFrozen() {
        if (frozen) {
            throw new IllegalStateException("Already frozen");
        }
    }
}
