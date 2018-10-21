/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;

import java.util.Collection;

/**
 * Chooses the optimal {@link IndexToIndexMultiMap} implementation
 *
 * @author svyatoslav
 * @author incubos
 */
public final class IndexToIndexMultiMapFactory {

    private IndexToIndexMultiMapFactory() {
        // Can't instantiate
    }

    // For test coverage
    static {
        new IndexToIndexMultiMapFactory();
    }

    private static boolean hasDocumentsWithMultipleKeys(
            final Collection<? extends Collection<Integer>> valueToDocuments,
            final int documentsCount) {
        int[] counters = new int[documentsCount];

        for (Collection<Integer> documents : valueToDocuments) {
            for (Integer document : documents) {
                int current = counters[document];
                if (current > 0) {
                    return false;
                }
                counters[document]++;
            }
        }

        return true;
    }

    public static IndexToIndexMultiMap buildIndexToIndexMultiMap(
            final Collection<? extends Collection<Integer>> valueToDocuments,
            final int documentsCount) {
        final int uniqueValuesCount = valueToDocuments.size();
        if (uniqueValuesCount == 0)
            throw new IllegalArgumentException("Nonpositive values count");
        if (documentsCount <= 0)
            throw new IllegalArgumentException("Nonpositive documents count");

        /**
         * If all of the documents have exactly one value we can use {@link AscendingBitSetIndexToIndexMultiMap}
         */
        if (hasDocumentsWithMultipleKeys(valueToDocuments, documentsCount)) {
            if (((long) uniqueValuesCount) * documentsCount / 64L <
                    documentsCount * 4L) {
                // BitSet might be more effective
                return new BitSetIndexToIndexMultiMap(
                        valueToDocuments,
                        documentsCount);
            } else {
                return new IntIndexToIndexMultiMap(valueToDocuments);
            }
        } else {
            return new AscendingBitSetIndexToIndexMultiMap(valueToDocuments, documentsCount);
        }
    }
}
