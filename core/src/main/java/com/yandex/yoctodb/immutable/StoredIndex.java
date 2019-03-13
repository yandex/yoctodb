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
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * Index storing field values
 *
 * @author incubos
 */
@Immutable
public interface StoredIndex extends Index {
    /**
     * Get stored value by document ID
     *
     * @param document document ID
     * @return stored value
     */
    @NotNull
    Buffer getStoredValue(int document);

    /**
     * Get stored value as long by document ID
     *
     * @param document document ID
     * @return stored value as long
     */
    long getLongValue(int document);

    /**
     * Get stored value as int by document ID
     *
     * @param document document ID
     * @return stored value as int
     */
    int getIntValue(int document);

    /**
     * Get stored value as short by document ID
     *
     * @param document document ID
     * @return stored value as short
     */
    short getShortValue(int document);

    /**
     * Get stored value as char by document ID
     *
     * @param document document ID
     * @return stored value as char
     */
    char getCharValue(int document);

    /**
     * Get stored value as byte by document ID
     *
     * @param document document ID
     * @return stored value as byte
     */
    byte getByteValue(int document);
}
