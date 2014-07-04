/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.mutable.segment;

import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.MessageDigestOutputStreamWrapper;
import ru.yandex.yoctodb.util.OutputStreamWritable;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import ru.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList;
import ru.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;

/**
 * Index supporting sorting by specific field
 *
 * @author incubos
 */
@NotThreadSafe
public final class V1SortableIndex
        extends Freezable
        implements IndexSegment {
    @NotNull
    private final byte[] fieldName;
    @NotNull
    private final ByteArrayIndexedList documentToValue;
    private int currentDocumentId = 0;
    private final boolean fixedLength;

    public V1SortableIndex(
            @NotNull
            final String fieldName,
            final boolean fixedLength) {
        assert !fieldName.isEmpty();

        this.fieldName = fieldName.getBytes();
        this.fixedLength = fixedLength;
        if (fixedLength) {
            this.documentToValue = new FixedLengthByteArrayIndexedList();
        } else {
            this.documentToValue = new VariableLengthByteArrayIndexedList();
        }
    }

    @NotNull
    @Override
    public IndexSegment addDocument(
            final int documentId,
            @NotNull
            final Collection<UnsignedByteArray> values) {
        assert documentId == currentDocumentId;
        assert values.size() == 1;

        checkNotFrozen();

        documentToValue.add(values.iterator().next());
        currentDocumentId++;

        return this;
    }

    @Override
    public void setDatabaseDocumentsCount(int documentsCount) {
        //do nothing
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        freeze();

        return new OutputStreamWritable() {
            @Override
            public int getSizeInBytes() {
                //without code and full size (8 bytes)
                return 4 + // Field name
                       fieldName.length +
                       4 + // Index
                       documentToValue.getSizeInBytes() +
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

                os.write(Ints.toByteArray(getSizeInBytes()));

                // Payload segment type
                os.write(
                        Ints.toByteArray(
                                fixedLength ?
                                        V1DatabaseFormat.SegmentType
                                                .FIXED_LENGTH_SORT
                                                .getCode() :
                                        V1DatabaseFormat.SegmentType
                                                .VARIABLE_LENGTH_SORT
                                                .getCode()
                        )
                );

                // With digest calculation
                final MessageDigestOutputStreamWrapper mdos =
                        new MessageDigestOutputStreamWrapper(os, md);

                // Field name
                mdos.write(Ints.toByteArray(fieldName.length));
                mdos.write(fieldName);

                // Index
                mdos.write(Ints.toByteArray(documentToValue.getSizeInBytes()));
                documentToValue.writeTo(mdos);

                //writing checksum
                assert V1DatabaseFormat.DIGEST_SIZE_IN_BYTES ==
                       md.getDigestLength();
                os.write(Ints.toByteArray(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES));
                os.write(mdos.digest());
            }
        };
    }
}
