package com.yandex.yoctodb;

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.query.DocumentProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class DoubleDatabaseProcessor implements DocumentProcessor {
    private final @NotNull Database negative;
    @NotNull
    private final Database positive;
    private final @NotNull List<Integer> docs;

    public DoubleDatabaseProcessor(
            @NotNull
            final Database negative,
            @NotNull
            final Database positive,
            @NotNull
            final List<Integer> docs) {
        this.negative = negative;
        this.positive = positive;
        this.docs = docs;
    }

    @Override
    public boolean process(
            final int document,
            @NotNull
            final Database database) {
        assert database == negative || database == positive;

        if (database == negative) {
            docs.add(-document);
        } else {
            docs.add(document);
        }

        return true;
    }
}
