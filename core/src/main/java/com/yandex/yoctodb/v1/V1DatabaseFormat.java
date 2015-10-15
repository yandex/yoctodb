/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
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

/**
 * First {@link DatabaseFormat} implementation
 *
 * @author incubos
 */
@ThreadSafe
public final class V1DatabaseFormat extends DatabaseFormat {
    public final static int FORMAT = 3;

    public final static DatabaseReader DATABASE_READER = new V1DatabaseReader();

    public final static String MESSAGE_DIGEST_ALGORITHM = "MD5";
    public final static long DIGEST_SIZE_IN_BYTES = 16;

    // Segment types
    public enum SegmentType {
        // External segments should start from 10E6
        PAYLOAD(1),
        FIXED_LENGTH_FILTER(1000),
        VARIABLE_LENGTH_FILTER(2000),
        TRIE_BASED_FILTER(2500),
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
        LIST_BASED(1000),
        LONG_ARRAY_BIT_SET_BASED(2000);

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
