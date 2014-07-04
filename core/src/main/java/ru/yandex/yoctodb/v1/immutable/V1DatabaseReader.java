/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.immutable;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.yoctodb.immutable.*;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;
import ru.yandex.yoctodb.v1.immutable.segment.Segment;
import ru.yandex.yoctodb.v1.immutable.segment.SegmentRegistry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds immutable {@link Database}s from bytes in V1 format
 *
 * @author incubos
 */
@ThreadSafe
public class V1DatabaseReader implements DatabaseReader {
    private static final Logger log =
            LoggerFactory.getLogger(V1DatabaseReader.class);

    @NotNull
    @Override
    public Database from(
            @NotNull
            final ByteBuffer buffer) throws IOException {
        // Checking the magic
        for (int i = 0; i < V1DatabaseFormat.MAGIC.length; i++)
            if (buffer.get() != V1DatabaseFormat.MAGIC[i]) {
                throw new IllegalArgumentException("Wrong magic");
            }

        // Checking the format version
        final int format = buffer.getInt();
        if (format != V1DatabaseFormat.FORMAT) {
            throw new IllegalArgumentException(
                    "Wrong format " + format + ". Supported format is " +
                    V1DatabaseFormat.FORMAT + ".");
        }

        // Reading the segments
        Payload payload = null;
        final Map<String, FilterableIndex> filters =
                new HashMap<String, FilterableIndex>();
        final Map<String, SortableIndex> sorters =
                new HashMap<String, SortableIndex>();
        while (buffer.hasRemaining()) {
            final int size = buffer.getInt();
            final int type = buffer.getInt();

            final ByteBuffer segmentBuffer = buffer.slice();
            segmentBuffer.limit(size);

            final Segment segment = SegmentRegistry.read(type, segmentBuffer);

            if (segment instanceof Payload) {
                if (payload != null) {
                    throw new IllegalArgumentException("Duplicate payload found");
                }

                payload = (Payload) segment;
            }

            if (segment instanceof FilterableIndex) {
                final FilterableIndex index = (FilterableIndex) segment;
                if (filters.containsKey(index.getFieldName())) {
                    throw new IllegalArgumentException(
                            "Duplicate filterable index for field <" +
                            index.getFieldName() + ">");
                } else {
                    filters.put(index.getFieldName(), index);
                }
            }

            if (segment instanceof SortableIndex) {
                final SortableIndex index = (SortableIndex) segment;
                if (sorters.containsKey(index.getFieldName())) {
                    throw new IllegalArgumentException(
                            "Duplicate sortable index for field <" +
                            index.getFieldName() + ">");
                } else {
                    sorters.put(index.getFieldName(), index);
                }
            }

            // Skipping read index
            buffer.position(buffer.position() + size);
        }

        if (payload == null) {
            throw new IllegalArgumentException("No payload found");
        }

        return new V1Database(payload, filters, sorters);
    }

    @NotNull
    @Override
    public Database composite(
            @NotNull
            final Collection<Database> databases) throws IOException {
        assert !databases.isEmpty();

        final Collection<V1Database> dbs =
                new ArrayList<V1Database>(databases.size());

        for (Database database : databases) {
            dbs.add((V1Database) database);
        }

        return new V1CompositeDatabase(dbs);
    }

    @NotNull
    @Override
    public Database from(
            @NotNull
            final File f,
            boolean forceToMemory) throws IOException {
        if (!f.exists()) {
            throw new IllegalArgumentException(
                    "File doesn't exist: " + f);
        }

        // Mapping the file
        final MappedByteBuffer buffer;
        final RandomAccessFile raf = new RandomAccessFile(f, "r");
        try {
            final FileChannel ch = raf.getChannel();
            try {
                buffer = ch.map(FileChannel.MapMode.READ_ONLY, 0, f.length());
            } finally {
                ch.close();
            }
        } finally {
            raf.close();
        }

        // Forcing data loading
        if (forceToMemory) {
            buffer.load();
            if (!buffer.isLoaded())
                log.warn("Couldn't force loading of file <{}> into memory", f);
        }

        // Setting byte order
        buffer.order(ByteOrder.BIG_ENDIAN);

        return from(buffer);
    }
}
