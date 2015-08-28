/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList;
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
public final class V1PayloadSegment
        extends Freezable
        implements PayloadSegment {
    @NotNull
    private final ByteArrayIndexedList payloads =
            new VariableLengthByteArrayIndexedList();
    private int currentDocumentId = 0;

    @NotNull
    @Override
    public PayloadSegment addDocument(
            final int documentId,
            @NotNull
            final byte[] payload) {
        if (documentId != currentDocumentId)
            throw new IllegalArgumentException(
                    "Wrong document ID <" + documentId + ">. Expecting <" +
                    currentDocumentId + ">.");

        checkNotFrozen();

        payloads.add(UnsignedByteArrays.raw(payload));
        currentDocumentId++;

        return this;
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        freeze();

        return new OutputStreamWritable() {
            @Override
            public long getSizeInBytes() {
                //without code and full size (8 bytes)
                return 8 + // Payload
                       payloads.getSizeInBytes();
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
                                        .PAYLOAD
                                        .getCode()
                        )
                );

                // data
                os.write(Longs.toByteArray(payloads.getSizeInBytes()));
                payloads.writeTo(os);
            }
        };
    }
}
