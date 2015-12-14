/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
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

    @NotNull
    Buffer getDocument(int i);

    /**
     * Get {@code document} document {@code fieldName} field value.
     *
     * The field must be sortable.
     *
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#SORTABLE
     * @see com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption#FULL
     * @param document  document index
     * @param fieldName field name
     * @return document field value
     */
    @NotNull
    Buffer getFieldValue(
            int document,
            @NotNull
            String fieldName);
}
