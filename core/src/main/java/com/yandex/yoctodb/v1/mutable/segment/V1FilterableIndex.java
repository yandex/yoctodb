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

import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.mutable.impl.IndexToIndexMultiMapFactory;
import com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet;
import com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

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
    private TreeMultimap<UnsignedByteArray, Integer> valueToDocuments =
            TreeMultimap.create();
    private final boolean fixedLength;
    private final boolean useTrie;
    private int databaseDocumentsCount = -1;

    public V1FilterableIndex(
            @NotNull
            final String fieldName,
            final boolean fixedLength) {
        this(fieldName, fixedLength, false);
    }

    public V1FilterableIndex(
            @NotNull
            final String fieldName,
            final boolean fixedLength,
            final boolean useTrie) {
        this.fieldName = fieldName.getBytes();
        this.fixedLength = fixedLength;
        this.useTrie = useTrie;
    }

    @NotNull
    @Override
    public IndexSegment addDocument(
            final int documentId,
            @NotNull
            final Collection<UnsignedByteArray> values) {
        if (documentId < 0)
            throw new IllegalArgumentException("Negative document ID");
        if (values.isEmpty())
            throw new IllegalArgumentException("No values");

        checkNotFrozen();

        for (UnsignedByteArray value : values) {
            valueToDocuments.put(value, documentId);
        }

        return this;
    }

    @Override
    public void setDatabaseDocumentsCount(final int documentsCount) {
        assert documentsCount > 0;

        this.databaseDocumentsCount = documentsCount;
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        checkNotFrozen();

        freeze();

        // Building the index
        final IndexToIndexMultiMap valueToDocumentsIndex =
                IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(
                        valueToDocuments.asMap().values(),
                        databaseDocumentsCount);

        final OutputStreamWritable values;
        if (useTrie) {
            values =
                    new TrieByteArraySortedSet(
                            valueToDocuments.keySet());
        } else if (fixedLength) {
            values =
                    new FixedLengthByteArraySortedSet(
                            valueToDocuments.keySet());
        } else {
            values =
                    new VariableLengthByteArraySortedSet(
                            valueToDocuments.keySet());
        }

        // Free memory
        valueToDocuments = null;

        return new OutputStreamWritable() {
            @Override
            public long getSizeInBytes() {
                return 4L + // Field name
                       fieldName.length +
                       8 + // Values
                       values.getSizeInBytes() +
                       8 + // Value to documents
                       valueToDocumentsIndex.getSizeInBytes();
            }

            @Override
            public void writeTo(
                    @NotNull
                    final OutputStream os) throws IOException {
                os.write(Longs.toByteArray(getSizeInBytes()));

                final V1DatabaseFormat.SegmentType segmentType;
                if (useTrie) {
                    segmentType = V1DatabaseFormat.SegmentType.TRIE_FILTER;
                } else if (fixedLength) {
                    segmentType = V1DatabaseFormat.SegmentType.FIXED_LENGTH_FILTER;
                } else {
                    segmentType = V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_FILTER;
                }

                // Payload segment type
                os.write(
                        Ints.toByteArray(
                                segmentType.getCode()
                        )
                );

                // Field name
                os.write(Ints.toByteArray(fieldName.length));
                os.write(fieldName);

                // Values
                os.write(Longs.toByteArray(values.getSizeInBytes()));
                values.writeTo(os);

                // Documents
                os.write(Longs.toByteArray(valueToDocumentsIndex.getSizeInBytes()));
                valueToDocumentsIndex.writeTo(os);
            }
        };
    }
}
