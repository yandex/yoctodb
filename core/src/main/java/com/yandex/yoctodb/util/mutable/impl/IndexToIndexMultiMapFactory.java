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
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.jetbrains.annotations.NotNull;

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

    public static boolean hasDocumentsWithMultipleKeys(
            @NotNull final Collection<? extends Collection<Integer>> valueToDocuments,
            final int documentsCount) {
        int[] counters = new int[documentsCount];

        for (Collection<Integer> documents : valueToDocuments) {
            for (Integer document : documents) {
                int current = counters[document];
                if (current > 0) {
                    return true;
                }
                counters[document]++;
            }
        }

        return false;
    }

    public static IndexToIndexMultiMap buildIndexToIndexMultiMap(
            @NotNull final Collection<? extends Collection<Integer>> valueToDocuments,
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
                return buildIndexToIndexMultiMap(
                        V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED,
                        valueToDocuments,
                        documentsCount);
            } else {
                return buildIndexToIndexMultiMap(
                        V1DatabaseFormat.MultiMapType.LIST_BASED,
                        valueToDocuments,
                        documentsCount);
            }
        } else {
            return new AscendingBitSetIndexToIndexMultiMap(valueToDocuments, documentsCount);
        }
    }

    public static IndexToIndexMultiMap buildIndexToIndexMultiMap(
            @NotNull final V1DatabaseFormat.MultiMapType type,
            @NotNull final Collection<? extends Collection<Integer>> valueToDocuments,
            final int documentsCount) {
        final int uniqueValuesCount = valueToDocuments.size();
        if (uniqueValuesCount == 0)
            throw new IllegalArgumentException("Nonpositive values count");
        if (documentsCount <= 0)
            throw new IllegalArgumentException("Nonpositive documents count");

        switch (type) {
            case LIST_BASED:
                return new IntIndexToIndexMultiMap(valueToDocuments);
            case LONG_ARRAY_BIT_SET_BASED:
                return new BitSetIndexToIndexMultiMap(valueToDocuments, documentsCount);
            case ASCENDING_BIT_SET_BASED:
                return buildIndexToIndexMultiMap(valueToDocuments, documentsCount);
            default:
                return buildIndexToIndexMultiMap(valueToDocuments, documentsCount);
        }
    }
}
