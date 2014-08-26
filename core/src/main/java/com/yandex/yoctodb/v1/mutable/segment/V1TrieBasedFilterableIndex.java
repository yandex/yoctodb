/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
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
 * @author svyatoslav Date: 12.04.14
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
            String fieldName) {
        assert !fieldName.isEmpty();
        this.fieldName = fieldName.getBytes();
        this.values = new SimpleTrieBasedByteArraySet();
        this.valueToDocuments = HashMultimap.create();
    }

    @NotNull
    @Override
    public IndexSegment addDocument(int documentId,
                                    @NotNull
                                    Collection<UnsignedByteArray> values) {
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
