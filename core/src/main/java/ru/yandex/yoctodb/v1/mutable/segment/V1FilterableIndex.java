/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.v1.mutable.segment;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.MessageDigestOutputStreamWrapper;
import ru.yandex.yoctodb.util.OutputStreamWritable;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import ru.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import ru.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet;
import ru.yandex.yoctodb.util.mutable.impl.IndexToIndexMultiMapFactory;
import ru.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;

/**
 * Index supporting filtering by specific field
 *
 * @author incubos
 */
@NotThreadSafe
public final class V1FilterableIndex
        extends Freezable
        implements IndexSegment {
    @NotNull
    private final byte[] fieldName;
    @NotNull
    private final ByteArraySortedSet values;
    @NotNull
    private final Multimap<UnsignedByteArray, Integer> valueToDocuments;
    private final boolean fixedLength;
    private int databaseDocumentsCount;

    public V1FilterableIndex(
            @NotNull
            final String fieldName,
            final boolean fixedLength) {
        assert !fieldName.isEmpty();

        this.fieldName = fieldName.getBytes();
        this.fixedLength = fixedLength;
        if (fixedLength) {
            this.values = new FixedLengthByteArraySortedSet();
        } else {
            this.values = new VariableLengthByteArraySortedSet();
        }
        this.valueToDocuments = HashMultimap.create();
    }

    @NotNull
    @Override
    public IndexSegment addDocument(
            final int documentId,
            @NotNull
            final Collection<UnsignedByteArray> values) {
        assert documentId >= 0;
        assert !values.isEmpty();

        checkNotFrozen();

        for (UnsignedByteArray value : values) {
            valueToDocuments.put(this.values.add(value), documentId);
        }

        return this;
    }

    @Override
    public void setDatabaseDocumentsCount(int documentsCount) {
        this.databaseDocumentsCount = documentsCount;
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        freeze();

        // Building the index
        final IndexToIndexMultiMap valueToDocumentsIndex =
                IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(
                        databaseDocumentsCount,
                        valueToDocuments.size());

        for (Map.Entry<UnsignedByteArray, Collection<Integer>> entry :
                valueToDocuments.asMap().entrySet()) {
            final int key = values.indexOf(entry.getKey());

            for (Integer d : entry.getValue()) {
                valueToDocumentsIndex.add(key, d);
            }
        }

        return new OutputStreamWritable() {
            @Override
            public int getSizeInBytes() {
                //without code and full size (8 bytes)
                return 4 + // Field name
                        fieldName.length +
                        4 + // Values
                        values.getSizeInBytes() +
                        4 + // Value to documents
                        valueToDocumentsIndex.getSizeInBytes() +
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
                                                .FIXED_LENGTH_FILTER
                                                .getCode() :
                                        V1DatabaseFormat.SegmentType
                                                .VARIABLE_LENGTH_FILTER
                                                .getCode()
                        )
                );

                // With digest calculation
                final MessageDigestOutputStreamWrapper mdos =
                        new MessageDigestOutputStreamWrapper(os, md);

                // Field name
                mdos.write(Ints.toByteArray(fieldName.length));
                mdos.write(fieldName);

                // Values
                mdos.write(Ints.toByteArray(values.getSizeInBytes()));
                values.writeTo(mdos);

                // Documents
                mdos.write(Ints.toByteArray(valueToDocumentsIndex.getSizeInBytes()));
                valueToDocumentsIndex.writeTo(mdos);

                //writing checksum
                assert V1DatabaseFormat.DIGEST_SIZE_IN_BYTES ==
                        md.getDigestLength();
                os.write(Ints.toByteArray(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES));
                os.write(mdos.digest());
            }
        };
    }
}
