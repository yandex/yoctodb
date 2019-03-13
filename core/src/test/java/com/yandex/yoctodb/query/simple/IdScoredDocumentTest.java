/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyZeroBitSet;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.NoSuchElementException;

/**
 * Unit tests for {@link com.yandex.yoctodb.query.simple.IdScoredDocument} and
 * {@link com.yandex.yoctodb.query.simple.IdScoredDocumentIterator}
 *
 * @author incubos
 */
public class IdScoredDocumentTest {
    private final Database db =
            new Database() {
                @Override
                public int getDocumentCount() {
                    return 1;
                }

                @NotNull
                @Override
                public Buffer getDocument(final int i) {
                    throw new IllegalStateException();
                }

                @NotNull
                @Override
                public Buffer getFieldValue(
                        final int document,
                        @NotNull
                        final String fieldName) {
                    throw new IllegalStateException();
                }

                @Override
                public long getLongValue(int document, @NotNull String fieldName) {
                    throw new IllegalStateException();
                }

                @Override
                public int getIntValue(int document, @NotNull String fieldName) {
                    throw new IllegalArgumentException();
                }

                @Override
                public short getShortValue(int document, @NotNull String fieldName) {
                    throw new IllegalArgumentException();
                }

                @Override
                public char getCharValue(int document, @NotNull String fieldName) {
                    throw new IllegalArgumentException();
                }

                @Override
                public byte getByteValue(int document, @NotNull String fieldName) {
                    throw new IllegalArgumentException();
                }

                @Override
                public void execute(
                        @NotNull
                        final Query query,
                        @NotNull
                        final DocumentProcessor processor) {
                    throw new IllegalStateException();
                }

                @Override
                public int executeAndUnlimitedCount(
                        @NotNull
                        final Query query,
                        @NotNull
                        final DocumentProcessor processor) {
                    throw new IllegalStateException();
                }

                @Override
                public int count(
                        @NotNull
                        final Query query) {
                    throw new IllegalStateException();
                }
            };

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedCompare() {
        final IdScoredDocument document = new IdScoredDocument(db, 0);

        document.compareTo(document);
    }

    @Test(expected = NoSuchElementException.class)
    public void nextOnEmptyIterator() {
        final IdScoredDocumentIterator iterator =
                new IdScoredDocumentIterator(
                        db,
                        new ReadOnlyZeroBitSet(db.getDocumentCount()));

        iterator.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedRemove() {
        final IdScoredDocumentIterator iterator =
                new IdScoredDocumentIterator(
                        db,
                        new ReadOnlyZeroBitSet(db.getDocumentCount()));

        iterator.remove();
    }
}
