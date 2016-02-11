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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Payload segment
 *
 * @author incubos
 */
@NotThreadSafe
public final class V1NonePayloadSegment
        extends Freezable
        implements PayloadSegment {
    private final int documentCount;

    public V1NonePayloadSegment(final int documentCount) {
        assert documentCount >= 0;

        this.documentCount = documentCount;
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        checkNotFrozen();

        freeze();

        return new OutputStreamWritable() {
            @Override
            public long getSizeInBytes() {
                return Ints.BYTES; // Document count
            }

            @Override
            public void writeTo(
                    @NotNull
                    final OutputStream os) throws IOException {
                // full size in bytes
                os.write(Longs.toByteArray(getSizeInBytes()));

                // Payload segment type
                os.write(
                        Ints.toByteArray(
                                V1DatabaseFormat.SegmentType
                                        .PAYLOAD_NONE
                                        .getCode()));

                // Document count
                os.write(Ints.toByteArray(documentCount));
            }
        };
    }
}
