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
import ru.yandex.yoctodb.util.mutable.IndexToIndexMap;
import ru.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import ru.yandex.yoctodb.util.mutable.impl.*;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Index supporting filtering and sorting by specific field
 *
 * @author incubos
 */
@NotThreadSafe
public final class V1FullIndex
        extends Freezable
        implements IndexSegment {
    @NotNull
    private final byte[] fieldName;
    @NotNull
    private final ByteArraySortedSet values;
    @NotNull
    private final Multimap<UnsignedByteArray, Integer> valueToDocuments;
    @NotNull
    private final Map<Integer, UnsignedByteArray> documentToValue;
    private int currentDocumentId = 0;
    private final boolean fixedLength;
    private int databaseDocumentsCount;

    public V1FullIndex(
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
        this.documentToValue = new HashMap<Integer, UnsignedByteArray>();
        this.valueToDocuments = HashMultimap.create();
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

        final UnsignedByteArray value = values.iterator().next();
        valueToDocuments.put(this.values.add(value), documentId);
        documentToValue.put(documentId, value);
        currentDocumentId++;

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

        // Building index

        final IndexToIndexMultiMap valueToDocumentsIndex =
                new IntIndexToIndexMultiMap();

        for (Map.Entry<UnsignedByteArray, Collection<Integer>> entry :
                valueToDocuments.asMap().entrySet()) {
            final int key = values.indexOf(entry.getKey());

            for (Integer d : entry.getValue()) {
                valueToDocumentsIndex.add(key, d);
            }
        }

        final IndexToIndexMap documentToValueIndex = new IntIndexToIndexMap();
        for (Map.Entry<Integer, UnsignedByteArray> entry :
                documentToValue.entrySet()) {
            documentToValueIndex.put(
                    entry.getKey(),
                    values.indexOf(entry.getValue()));
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
                        4 + // Document to value
                        documentToValueIndex.getSizeInBytes() +
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
                                                .FIXED_LENGTH_FULL_INDEX
                                                .getCode() :
                                        V1DatabaseFormat.SegmentType
                                                .VARIABLE_LENGTH_FULL_INDEX
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

                // Value to documents
                mdos.write(Ints.toByteArray(valueToDocumentsIndex.getSizeInBytes()));
                valueToDocumentsIndex.writeTo(mdos);

                // Document to value
                mdos.write(Ints.toByteArray(documentToValueIndex.getSizeInBytes()));
                documentToValueIndex.writeTo(mdos);

                //writing checksum
                assert V1DatabaseFormat.DIGEST_SIZE_IN_BYTES ==
                        md.getDigestLength();
                os.write(Ints.toByteArray(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES));
                os.write(mdos.digest());
            }
        };
    }
}
