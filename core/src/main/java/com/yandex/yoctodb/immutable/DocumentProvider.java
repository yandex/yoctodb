/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

/**
 * Provides access to database documents
 *
 * @author incubos
 */
public interface DocumentProvider {
    int getDocumentCount();

    /**
     * Get document payload
     *
     * @deprecated use stored fields
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#STORED
     * @param i document ID
     * @return payload
     */
    @Deprecated
    @NotNull
    Buffer getDocument(int i);

    /**
     * Get {@code document} document {@code fieldName} field value.
     *
     * The field must be sortable, stored or full.
     *
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#SORTABLE
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#FULL
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#STORED
     * @param document  document index
     * @param fieldName field name
     * @return document field value
     */
    @NotNull
    Buffer getFieldValue(
            int document,
            @NotNull
            String fieldName);

    /**
     * Get {@code document} document {@code fieldName} field value as long.
     *
     * The field must be sortable, stored or full.
     *
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#SORTABLE
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#FULL
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#STORED
     * @param document document index
     * @param fieldName field name
     * @return document field value as long
     */
    long getLongValue(
            int document,
            @NotNull
            String fieldName);

    /**
     * Get {@code document} document {@code fieldName} field value as int.
     *
     * The field must be sortable, stored or full.
     *
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#SORTABLE
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#FULL
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#STORED
     * @param document document index
     * @param fieldName field name
     * @return document field value as int
     */
    int getIntValue(
            int document,
            @NotNull
            String fieldName);

    /**
     * Get {@code document} document {@code fieldName} field value as short.
     *
     * The field must be sortable, stored or full.
     *
     * @param document document index
     * @param fieldName field name
     * @return document field value as short
     */
    short getShortValue(
            int document,
            @NotNull
            String fieldName
    );

    /**
     * Get {@code document} document {@code fieldName} field value as char.
     *
     * The field must be sortable, stored or full.
     *
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#SORTABLE
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#FULL
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#STORED
     * @param document document index
     * @param fieldName field name
     * @return document field value as char
     */
    char getCharValue(
            int document,
            @NotNull
            String fieldName
    );

    /**
     * Get {@code document} document {@code fieldName} field value as byte.
     *
     * The field must be sortable, stored or full.
     *
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#SORTABLE
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#FULL
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#STORED
     * @param document document index
     * @param fieldName field name
     * @return document field value as byte
     */
    byte getByteValue(
            int document,
            @NotNull
            String fieldName
    );
}
