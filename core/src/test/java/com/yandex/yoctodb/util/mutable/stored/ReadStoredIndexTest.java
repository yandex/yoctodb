package com.yandex.yoctodb.util.mutable.stored;

import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import org.junit.Test;
import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.STORED;


import java.io.IOException;

public class ReadStoredIndexTest {

    @Test
    public void buildWithAllSupportedDataTypes() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        dbBuilder.merge(buildTestDocument("v1"))
                .merge(buildTestDocument("v1"))
                .merge(buildTestDocument("v2"))
                .merge(buildTestDocument("v1"))
                .merge(buildTestDocument("v2"))
                .merge(buildTestDocument("v1"))
                .merge(buildTestDocument("v1"))
                .merge(buildTestDocument("v3"))
                .merge(buildTestDocument("v3"))
                .merge(buildTestDocument("v1"));

        dbBuilder.buildWritable();
        assert true;


    }

    private DocumentBuilder buildTestDocument(String value) {
        return DatabaseFormat.getCurrent().newDocumentBuilder()
                .withField("test", value, STORED);
    }
}
