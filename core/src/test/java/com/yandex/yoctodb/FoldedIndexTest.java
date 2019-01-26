package com.yandex.yoctodb;

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FOLDED;
import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FULL;

public class FoldedIndexTest {

    @Test
    public void buildDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("state", "NEW", FOLDED)
                        .withField("state", "USED", FOLDED)
                        .withField("state", "NEW", FOLDED));

        OutputStreamWritable writable = dbBuilder.buildWritable();

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));


    }

}
