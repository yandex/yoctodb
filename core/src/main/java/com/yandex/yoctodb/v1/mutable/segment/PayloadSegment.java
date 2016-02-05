/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.OutputStreamWritableBuilder;

/**
 * Contains document payload
 *
 * @author incubos
 */
public interface PayloadSegment extends OutputStreamWritableBuilder {
    @NotNull
    PayloadSegment addDocument(
            final int documentId,
            @NotNull
            final byte[] payload);
}
