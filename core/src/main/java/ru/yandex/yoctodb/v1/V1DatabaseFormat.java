/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.DatabaseFormat;
import ru.yandex.yoctodb.immutable.DatabaseReader;
import ru.yandex.yoctodb.mutable.DatabaseBuilder;
import ru.yandex.yoctodb.mutable.DocumentBuilder;
import ru.yandex.yoctodb.v1.immutable.V1DatabaseReader;
import ru.yandex.yoctodb.v1.mutable.V1DatabaseBuilder;
import ru.yandex.yoctodb.v1.mutable.V1DocumentBuilder;

/**
 * First {@link DatabaseFormat} implementation
 *
 * @author incubos
 */
@ThreadSafe
public final class V1DatabaseFormat extends DatabaseFormat {
    public final static int FORMAT = 1;

    public final static DatabaseReader DATABASE_READER = new V1DatabaseReader();

    public final static String MESSAGE_DIGEST_ALGORITHM = "MD5";
    public final static int DIGEST_SIZE_IN_BYTES = 16;

    // Segment types
    public enum SegmentType {
        // External segments should start from 10E6
        PAYLOAD(1),
        FIXED_LENGTH_FILTER(1000),
        VARIABLE_LENGTH_FILTER(2000),
        TRIE_BASED_FILTER(2500),
        FIXED_LENGTH_SORT(3000),
        VARIABLE_LENGTH_SORT(4000),
        FIXED_LENGTH_FULL_INDEX(5000),
        VARIABLE_LENGTH_FULL_INDEX(6000);

        private final int code;

        private SegmentType(final int code) {
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

        private MultiMapType(final int code) {
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
