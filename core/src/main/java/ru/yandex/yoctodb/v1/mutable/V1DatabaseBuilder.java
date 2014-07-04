/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.v1.mutable;

import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.DatabaseFormat;
import ru.yandex.yoctodb.mutable.DatabaseBuilder;
import ru.yandex.yoctodb.mutable.DocumentBuilder;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.OutputStreamWritable;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;
import ru.yandex.yoctodb.v1.mutable.segment.*;

import java.io.IOException;
import java.io.OutputStream;
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

    private final V1PayloadSegment payloads = new V1PayloadSegment();

    private final Map<String, IndexSegment> indexes =
            new HashMap<String, IndexSegment>();

    @NotNull
    @Override
    public DatabaseBuilder merge(
            @NotNull
            final DocumentBuilder document) {
        checkNotFrozen();

        assert document instanceof V1DocumentBuilder :
                "Wrong document builder implementation supplied";

        final V1DocumentBuilder builder = (V1DocumentBuilder) document;

        // Checking all the necessary fields
        builder.check();

        // Marking document as built
        builder.markBuilt();

        // Updating the indexes

        // pass fixed or variable to index

        for (Map.Entry<String, Collection<UnsignedByteArray>> e :
                builder.fields.asMap().entrySet()) {
            final String fieldName = e.getKey();
            assert !fieldName.isEmpty();

            final Collection<UnsignedByteArray> values = e.getValue();
            assert !e.getValue().isEmpty();

            final IndexSegment existingIndex = indexes.get(fieldName);
            if (existingIndex == null) {
                final IndexSegment index;
                @NotNull
                final DocumentBuilder.IndexOption indexOption =
                        builder.index.get(fieldName);
                @NotNull
                final DocumentBuilder.LengthOption lengthOption =
                        builder.length.get(fieldName);

                switch (indexOption) {
                    case FILTERABLE:
                        index = new V1FilterableIndex(
                                fieldName,
                                lengthOption ==
                                        DocumentBuilder.LengthOption.FIXED
                        );
                        break;
                    case FILTERABLE_TRIE_BASED:
                        index = new V1TrieBasedFilterableIndex(
                                fieldName
                        );
                        break;
                    case SORTABLE:
                        index = new V1FullIndex(
                                fieldName,
                                lengthOption ==
                                        DocumentBuilder.LengthOption.FIXED
                        );
                        break;
                    case FULL:
                        index = new V1FullIndex(
                                fieldName,
                                lengthOption ==
                                        DocumentBuilder.LengthOption.FIXED
                        );
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

        // Adding payload and moving on
        payloads.addDocument(currentDocumentId, builder.payload);
        currentDocumentId++;

        return this;
    }

    @NotNull
    @Override
    public OutputStreamWritable buildWritable() {
        freeze();

        // Build writables
        final List<OutputStreamWritable> writables =
                new ArrayList<OutputStreamWritable>(indexes.size() + 1);
        for (IndexSegment segment : indexes.values()) {
            segment.setDatabaseDocumentsCount(currentDocumentId);
            writables.add(segment.buildWritable());
        }
        writables.add(payloads.buildWritable());

        return new OutputStreamWritable() {
            @Override
            public int getSizeInBytes() {
                // Magic and format
                int size = 4 + 4;

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
                os.write(Ints.toByteArray(V1DatabaseFormat.FORMAT));

                // Segments
                for (OutputStreamWritable writable : writables) {
                    writable.writeTo(os);
                }
            }
        };
    }
}
