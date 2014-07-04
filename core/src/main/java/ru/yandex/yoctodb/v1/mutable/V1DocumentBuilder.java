/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.mutable;

import com.google.common.collect.TreeMultimap;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.mutable.AbstractDocumentBuilder;
import ru.yandex.yoctodb.mutable.DocumentBuilder;
import ru.yandex.yoctodb.util.UnsignedByteArray;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link DocumentBuilder} implementation in V1 format
 *
 * @author incubos
 */
@NotThreadSafe
public final class V1DocumentBuilder extends AbstractDocumentBuilder {
    byte[] payload = null;

    final TreeMultimap<String, UnsignedByteArray> fields = TreeMultimap.create();
    final Map<String, IndexOption> index = new HashMap<String, IndexOption>();
    final Map<String, LengthOption> length = new HashMap<String, LengthOption>();

    @NotNull
    @Override
    public DocumentBuilder withPayload(
            @NotNull
            final byte[] payload) {
        checkNotBuilt();

        if (this.payload != null) {
            throw new IllegalStateException("The payload is already set");
        }

        this.payload = payload;

        return this;
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            @NotNull
            final UnsignedByteArray value,
            @NotNull
            final IndexOption index,
            @NotNull
            final LengthOption length) {
        checkNotBuilt();

        this.fields.put(name, value);
        final IndexOption previousIndex = this.index.put(name, index);
        assert previousIndex == null || previousIndex == index;
        final LengthOption previousLength = this.length.put(name, length);
        assert previousLength == null || previousLength == length;

        return this;
    }

    @Override
    public void check() {
        checkNotBuilt();

        if (payload == null) {
            throw new IllegalArgumentException("The payload is not set");
        }

        if (fields.isEmpty()) {
            throw new IllegalArgumentException("No fields in the document");
        }
    }

    void markBuilt() {
        built = true;
    }

    private boolean built = false;

    private void checkNotBuilt() {
        if (built) {
            throw new IllegalStateException(
                    "The builder can't be used anymore");
        }
    }
}
