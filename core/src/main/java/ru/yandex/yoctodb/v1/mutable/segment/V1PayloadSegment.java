/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.v1.mutable.segment;

import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.MessageDigestOutputStreamWrapper;
import ru.yandex.yoctodb.util.OutputStreamWritable;
import ru.yandex.yoctodb.util.UnsignedByteArrays;
import ru.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import ru.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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
        assert currentDocumentId == documentId;

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
            public int getSizeInBytes() {
                //without code and full size (8 bytes)
                return 4 +// Payload
                       payloads.getSizeInBytes() +
                       4 + //checksum
                       V1DatabaseFormat.DIGEST_SIZE_IN_BYTES;
            }

            @Override
            public void writeTo(
                    @NotNull
                    final OutputStream os) throws IOException {
                final MessageDigest md;
                try {
                    md = MessageDigest.getInstance(
                            V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
                md.reset();

                // full size in bytes
                os.write(Ints.toByteArray(getSizeInBytes()));

                // Payload segment type
                os.write(
                        Ints.toByteArray(
                                V1DatabaseFormat.SegmentType
                                        .PAYLOAD
                                        .getCode()
                        )
                );

                // With digest calculation
                final MessageDigestOutputStreamWrapper mdos =
                        new MessageDigestOutputStreamWrapper(os, md);

                // data
                mdos.write(Ints.toByteArray(payloads.getSizeInBytes()));
                payloads.writeTo(mdos);

                //writing checksum
                assert V1DatabaseFormat.DIGEST_SIZE_IN_BYTES ==
                       md.getDigestLength();
                os.write(Ints.toByteArray(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES));
                os.write(mdos.digest());
            }
        };
    }
}
