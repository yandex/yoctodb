/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reads segments
 *
 * @author incubos
 */
@Immutable
public interface SegmentReader {
    @NotNull
    Segment read(
            @NotNull
            ByteBuffer buffer) throws IOException;
}
