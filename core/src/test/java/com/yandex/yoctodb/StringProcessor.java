/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb;

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * String processor collecting documents in {@link List} to be used in
 * unit tests
 *
 * @author incubos
 */
class StringProcessor implements DocumentProcessor {
    private final List<String> results;

    public StringProcessor(final List<String> results) {
        this.results = results;
    }

    @Override
    public boolean process(
            final int document,
            final @NotNull Database database) {
        final Buffer payload = database.getDocument(document);
        final byte[] buf = new byte[(int) payload.remaining()];
        payload.get(buf);
        results.add(new String(buf));
        return true;
    }
}
