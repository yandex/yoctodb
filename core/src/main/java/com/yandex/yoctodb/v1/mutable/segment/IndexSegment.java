/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
            final int documentId,
            @NotNull
            final Collection<UnsignedByteArray> values);

    void setDatabaseDocumentsCount(int documentsCount);
}
