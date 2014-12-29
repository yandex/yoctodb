/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.OutputStreamWritableBuilder;

import java.util.Collection;

/**
 * Mutable index segment
 *
 * @author incubos
 */
@NotThreadSafe
public interface IndexSegment extends OutputStreamWritableBuilder {
    @NotNull
    IndexSegment addDocument(
            int documentId,
            @NotNull
            Collection<UnsignedByteArray> values);

    void setDatabaseDocumentsCount(int documentsCount);
}
