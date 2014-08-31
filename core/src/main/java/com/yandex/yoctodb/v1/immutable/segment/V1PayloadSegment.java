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

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.Payload;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArrayIndexedList;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;

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

    @Override
    public String toString() {
        return "V1PayloadSegment{" +
                "documentCount=" + payloads.size() +
                '}';
    }

    static void registerReader() {
        SegmentRegistry.register(
                V1DatabaseFormat.SegmentType.PAYLOAD.getCode(),
                new SegmentReader() {
                    @NotNull
                    @Override
                    public Segment read(
                            @NotNull
                            final Buffer buffer) throws IOException {
                        final Buffer digest =
                                Segments.calculateDigest(
                                        buffer,
                                        V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

                        final ByteArrayIndexedList payloads =
                                VariableLengthByteArrayIndexedList.from(
                                        Segments.extract(buffer));

                        final Buffer digestActual = Segments.extract(buffer);
                        if (!digestActual.equals(digest)) {
                            throw new CorruptSegmentException("checksum error");
                        }

                        return new V1PayloadSegment(payloads);
                    }
                });
    }
}
