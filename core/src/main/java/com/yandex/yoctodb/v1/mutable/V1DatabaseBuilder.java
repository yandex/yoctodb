/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable;

import com.google.common.primitives.Ints;
import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.util.MessageDigestOutputStreamWrapper;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import com.yandex.yoctodb.v1.V1DatabaseFormat.Feature;
import com.yandex.yoctodb.v1.mutable.segment.*;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * {@link DatabaseBuilder} implementation in V1 format
 *
 * @author incubos
 */
@NotThreadSafe
public final class V1DatabaseBuilder
        extends Freezable
        implements DatabaseBuilder {
    private int currentDocumentId = 0;

    private final Map<String, IndexSegment> indexes =
            new HashMap<>();

    @NotNull
    @Override
    public DatabaseBuilder merge(
            @NotNull
            final DocumentBuilder document) {
        checkNotFrozen();

        assert document instanceof V1DocumentBuilder :
                "Wrong document builder implementation supplied";

        final V1DocumentBuilder builder = (V1DocumentBuilder) document;

        // Marking document as built
        builder.freeze();

        // Updating the indexes

        // pass fixed or variable to index

        for (Map.Entry<String, Collection<UnsignedByteArray>> e :
                builder.fields.asMap().entrySet()) {
            final String fieldName = e.getKey();
            final Collection<UnsignedByteArray> values = e.getValue();

            final IndexSegment existingIndex = indexes.get(fieldName);
            if (existingIndex == null) {
                final IndexSegment index;
                @NotNull
                final DocumentBuilder.IndexOption indexOption =
                        builder.index.get(fieldName);
                @NotNull
                final DocumentBuilder.IndexType indexType =
                        builder.length.get(fieldName);

                switch (indexOption) {
                    case FILTERABLE:
                        index = new V1FilterableIndex(
                                fieldName,
                                indexType == DocumentBuilder.IndexType.FIXED_LENGTH,
                                indexType == DocumentBuilder.IndexType.TRIE
                        );
                        break;
                    case SORTABLE:
                        index = new V1SortableIndex(
                                fieldName,
                                indexType == DocumentBuilder.IndexType.FIXED_LENGTH
                        );
                        break;
                    case FULL:
                        index = new V1FullIndex(
                                fieldName,
                                indexType == DocumentBuilder.IndexType.FIXED_LENGTH
                        );
                        break;
                    case STORED:
                        index = new V1StoredIndex(fieldName);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported index option: " + indexOption);
                }

                indexes.put(fieldName, index);
                index.addDocument(currentDocumentId, values);
            } else {
                existingIndex.addDocument(currentDocumentId, values);
            }
        }

        currentDocumentId++;

        return this;
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        checkNotFrozen();

        freeze();

        // Build writables

        final List<OutputStreamWritable> writables =
                new ArrayList<>(indexes.size() + 1);
        final Iterator<IndexSegment> indexSegmentIterator =
                indexes.values().iterator();
        while (indexSegmentIterator.hasNext()) {
            final IndexSegment segment = indexSegmentIterator.next();
            segment.setDatabaseDocumentsCount(currentDocumentId);
            writables.add(segment.buildWritable());
            indexSegmentIterator.remove();
        }

        return new OutputStreamWritable() {
            @Override
            public long getSizeInBytes() {
                long size =
                        V1DatabaseFormat.MAGIC.length +
                        Ints.BYTES + // Format length
                        Ints.BYTES + // Document count
                        V1DatabaseFormat.getDigestSizeInBytes();

                for (OutputStreamWritable writable : writables) {
                    size += writable.getSizeInBytes();
                }

                return size;
            }

            @Override
            public void writeTo(
                    @NotNull
                    final OutputStream os) throws IOException {
                // Header
                os.write(DatabaseFormat.MAGIC);
                os.write(Ints.toByteArray(
                    Feature.intValue(
                        Feature.ASCENDING_BIT_SET_INDEX,
                        Feature.TRIE_BYTE_ARRAY_SORTED_SET
                    )
                ));
                os.write(Ints.toByteArray(currentDocumentId));

                final MessageDigest md;
                try {
                    md = MessageDigest.getInstance(
                            V1DatabaseFormat.getMessageDigestAlgorithm());
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }

                md.reset();

                // With digest calculation
                final MessageDigestOutputStreamWrapper mdos =
                        new MessageDigestOutputStreamWrapper(os, md);

                // Segments
                for (OutputStreamWritable writable : writables) {
                    writable.writeTo(mdos);
                }

                // Writing checksum
                if (V1DatabaseFormat.getDigestSizeInBytes() !=
                    md.getDigestLength()) {
                    throw new IllegalStateException(
                            "Wrong digest size (" +
                            V1DatabaseFormat.getDigestSizeInBytes() +
                            " != " + md.getDigestLength() + ")");
                }

                os.write(mdos.digest());
            }
        };
    }
}
