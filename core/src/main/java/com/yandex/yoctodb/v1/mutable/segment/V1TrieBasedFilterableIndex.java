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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.MessageDigestOutputStreamWrapper;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.mutable.TrieBasedByteArraySet;
import com.yandex.yoctodb.util.mutable.impl.IndexToIndexMultiMapFactory;
import com.yandex.yoctodb.util.mutable.impl.SimpleTrieBasedByteArraySet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;

/**
 * @author svyatoslav
 */
@NotThreadSafe
public class V1TrieBasedFilterableIndex extends Freezable
        implements IndexSegment {

    @NotNull
    private final byte[] fieldName;
    @NotNull
    private final TrieBasedByteArraySet values;
    @NotNull
    private final Multimap<UnsignedByteArray, Integer> valueToDocuments;
    private int databaseDocumentsCount;

    public V1TrieBasedFilterableIndex(
            @NotNull
            final String fieldName) {
        if (fieldName.isEmpty())
            throw new IllegalArgumentException("Empty field name");

        this.fieldName = fieldName.getBytes();
        this.values = new SimpleTrieBasedByteArraySet();
        this.valueToDocuments = HashMultimap.create();
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
            throw new IllegalArgumentException("Empty values");

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
            public long getSizeInBytes() {
                //without code and full size (8 bytes)
                final long result =
                        4 + // Field name
                        fieldName.length +
                        8 + // Values
                        values.getSizeInBytes() +
                        8 + // Value to documents
                        valueToDocumentsIndex.getSizeInBytes() +
                        8 + //checksum
                        V1DatabaseFormat.DIGEST_SIZE_IN_BYTES;

                assert result <= Integer.MAX_VALUE : "Segment reached 2GB";

                return result;
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

                os.write(Longs.toByteArray(getSizeInBytes()));

                // Payload segment type
                os.write(
                        Ints.toByteArray(
                                V1DatabaseFormat.SegmentType
                                        .TRIE_BASED_FILTER
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
                mdos.write(Longs.toByteArray(values.getSizeInBytes()));
                values.writeTo(mdos);

                // Documents
                mdos.write(Longs.toByteArray(valueToDocumentsIndex.getSizeInBytes()));
                valueToDocumentsIndex.writeTo(mdos);

                //writing checksum
                if (V1DatabaseFormat.DIGEST_SIZE_IN_BYTES !=
                        md.getDigestLength())
                    throw new IllegalArgumentException("Wrong digest size");
                os.write(Longs.toByteArray(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES));
                os.write(mdos.digest());
            }
        };
    }
}
