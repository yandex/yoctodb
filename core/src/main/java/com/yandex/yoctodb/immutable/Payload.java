/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Payload segment interface
 *
 * @author incubos
 */
@Immutable
public interface Payload {
    int getSize();

    @NotNull
    ByteBuffer getPayload(final int i);
}
