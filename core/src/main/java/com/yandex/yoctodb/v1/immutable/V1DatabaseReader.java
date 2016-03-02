/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable;

import com.yandex.yoctodb.immutable.*;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import com.yandex.yoctodb.v1.immutable.segment.Segment;
import com.yandex.yoctodb.v1.immutable.segment.SegmentRegistry;
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds immutable {@link Database}s from bytes in V1 format
 *
 * @author incubos
 */
@ThreadSafe
public class V1DatabaseReader extends DatabaseReader {
    private static final int DIGEST_BUF_SIZE = 4096;

    private static Buffer calculateDigest(
            @NotNull
            final Buffer buffer) {
        final Buffer data = buffer.slice();

        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(
                    V1DatabaseFormat.getMessageDigestAlgorithm());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.reset();

        final byte[] buf = new byte[DIGEST_BUF_SIZE];

        while (data.remaining() >= buf.length) {
            data.get(buf);
            md.update(buf);
        }

        if (data.hasRemaining()) {
            md.update(data.toByteArray());
        }

        return Buffer.from(md.digest());
    }

    @NotNull
    @Override
    public IndexedDatabase from(
            @NotNull
            final Buffer buffer,
            @NotNull
            final ArrayBitSetPool bitSetPool,
            final boolean checksum) throws IOException {
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

        if (buffer.remaining() < V1DatabaseFormat.getDigestSizeInBytes()) {
            throw new IllegalArgumentException("Too small buffer");
        }

        final Buffer body =
                buffer.slice(
                        buffer.remaining() -
                        V1DatabaseFormat.getDigestSizeInBytes());

        if (checksum) {
            final Buffer originalDigest =
                    buffer.slice(
                            buffer.position() + body.remaining(),
                            V1DatabaseFormat.getDigestSizeInBytes());
            final Buffer currentDigest = calculateDigest(body);
            if (!currentDigest.equals(originalDigest)) {
                throw new IllegalArgumentException(
                        "The database is corrupted");
            }
        }

        // Reading the segments
        Payload payload = null;
        final Map<String, FilterableIndex> filters =
                new HashMap<String, FilterableIndex>();
        final Map<String, SortableIndex> sorters =
                new HashMap<String, SortableIndex>();
        while (body.hasRemaining()) {
            final long size = body.getLong();
            final int type = body.getInt();

            final Buffer segmentBuffer = body.slice(size);

            final Segment segment = SegmentRegistry.read(type, segmentBuffer);

            if (segment instanceof Payload) {
                assert payload == null : "Duplicate payload found";

                payload = (Payload) segment;
            }

            if (segment instanceof FilterableIndex) {
                final FilterableIndex index = (FilterableIndex) segment;
                final String name = index.getFieldName();

                assert !filters.containsKey(name) :
                        "Duplicate filterable index for field <" + name + ">";

                filters.put(name, index);
            }

            if (segment instanceof SortableIndex) {
                final SortableIndex index = (SortableIndex) segment;
                final String name = index.getFieldName();

                assert !sorters.containsKey(name) :
                        "Duplicate sortable index for field <" + name + ">";

                sorters.put(name, index);
            }

            // Skipping read index
            body.position(body.position() + size);
        }

        assert payload != null : "No payload found";

        return new V1Database(payload, filters, sorters, bitSetPool);
    }

    @NotNull
    @Override
    public Database composite(
            @NotNull
            final Collection<? extends IndexedDatabase> databases,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        return new V1CompositeDatabase(databases, bitSetPool);
    }
}
