/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.immutable.segment;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.Payload;
import ru.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import ru.yandex.yoctodb.util.immutable.impl.VariableLengthByteArrayIndexedList;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.nio.ByteBuffer;

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
    public ByteBuffer getPayload(final int i) {
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
                            final ByteBuffer buffer) throws IOException {
                        final byte[] digest = Segments.calculateDigest(buffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

                        final ByteArrayIndexedList payloads = VariableLengthByteArrayIndexedList.from(Segments.extract(buffer));

                        final ByteBuffer digestActual = Segments.extract(buffer);
                        if (!digestActual.equals(ByteBuffer.wrap(digest))) {
                            throw new CorruptSegmentException("checksum error");
                        }

                        return new V1PayloadSegment(payloads);
                    }
                });
    }
}
