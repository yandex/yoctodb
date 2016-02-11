/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.immutable.Payload;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

/**
 * Immutable payload segment containing only document count.
 *
 * No documents have any payload, so it throws {@link NoSuchElementException}.
 *
 * @author incubos
 */
@Immutable
public final class V1NonePayloadSegment implements Payload, Segment {
    private final int size;

    private V1NonePayloadSegment(final int size) {
        assert size >= 0;

        this.size = size;
    }

    @Override
    public int getSize() {
        return size;
    }

    @NotNull
    @Override
    public Buffer getPayload(final int i) {
        assert 0 <= i && i < size;

        throw new NoSuchElementException("No payload");
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.PAYLOAD_NONE.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) {
                        return new V1NonePayloadSegment(buffer.getInt());
                    }
                });
    }
}
