/*
 * (C) YANDEX LLC, 2014-2015
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
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArrayIndexedList;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable payload segment of V1 format
 *
 * @author incubos
 */
@Immutable
public final class V1PayloadSegment implements Payload, Segment {
    @NotNull
    private final ByteArrayIndexedList payloads;

    private V1PayloadSegment(
            @NotNull
            final ByteArrayIndexedList payloads) {
        this.payloads = payloads;
    }

    @Override
    public int getSize() {
        return payloads.size();
    }

    @NotNull
    @Override
    public Buffer getPayload(final int i) {
        assert 0 <= i && i < payloads.size();

        return payloads.get(i);
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.PAYLOAD.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) {
                        final ByteArrayIndexedList payloads =
                                VariableLengthByteArrayIndexedList.from(
                                        Segments.extract(buffer));

                        return new V1PayloadSegment(payloads);
                    }
                });
    }
}
