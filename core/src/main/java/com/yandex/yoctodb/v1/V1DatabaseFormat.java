/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.immutable.DatabaseReader;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.v1.immutable.V1DatabaseReader;
import com.yandex.yoctodb.v1.mutable.V1DatabaseBuilder;
import com.yandex.yoctodb.v1.mutable.V1DocumentBuilder;

import java.util.concurrent.atomic.AtomicReference;

/**
 * First {@link DatabaseFormat} implementation
 *
 * @author incubos
 */
@ThreadSafe
public final class V1DatabaseFormat extends DatabaseFormat {
    public final static int FORMAT = 6;

    private final static DatabaseReader DATABASE_READER = new V1DatabaseReader();

    private final static AtomicReference<String> messageDigestAlgorithm =
            new AtomicReference<>("MD5");

    @NotNull
    public static String getMessageDigestAlgorithm() {
        return messageDigestAlgorithm.get();
    }

    public static void setMessageDigestAlgorithm(
            @NotNull
            final String algorithm) {
        messageDigestAlgorithm.set(algorithm);
    }

    private final static AtomicReference<Integer> digestSize =
            new AtomicReference<>(16); // MD5 size

    @NotNull
    public static Integer getDigestSizeInBytes() {
        return digestSize.get();
    }

    public static void setDigestSizeInBytes(final int size) {
        assert size > 0;

        digestSize.set(size);
    }

    // Segment types
    public enum SegmentType {
        // External segments should start from 10E6
        PAYLOAD_FULL(1), // All documents have payload
        PAYLOAD_NONE(2), // No documents have any payload
        FIXED_LENGTH_FILTER(1000),
        VARIABLE_LENGTH_FILTER(2000),
        FIXED_LENGTH_SORTABLE_INDEX(3000),
        VARIABLE_LENGTH_SORTABLE_INDEX(4000),
        FIXED_LENGTH_FULL_INDEX(5000),
        VARIABLE_LENGTH_FULL_INDEX(6000);

        private final int code;

        SegmentType(final int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public enum MultiMapType {
        ROARING_BIT_SET_BASED(3000);

        private final int code;

        MultiMapType(final int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    @NotNull
    @Override
    public DocumentBuilder newDocumentBuilder() {
        return new V1DocumentBuilder();
    }

    @NotNull
    @Override
    public DatabaseBuilder newDatabaseBuilder() {
        return new V1DatabaseBuilder();
    }

    @NotNull
    @Override
    public DatabaseReader getDatabaseReader() {
        return DATABASE_READER;
    }
}
