package org.apache.iceberg.io;

import org.junit.Test;

import java.util.Arrays;

import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CatalogFile;
import org.apache.iceberg.io.CatalogFile.TableStruct;

public class TestCatalogFile {
    // Test our ability to write and read a CatalogFile
    @Test
    public void testCatalogFile() throws Exception {
        TableStruct table = new TableStruct("ns", "name", "loc", "meta");
        TableStruct table2 = new TableStruct("ns2", "name2", "loc2", "meta2");
        CatalogFile catalogFile = new CatalogFile(Arrays.asList(table, table2));
        CatalogFile.SimpleWriter writer = new CatalogFile.SimpleWriter();
        InMemoryOutputFile out = new InMemoryOutputFile("catalog.avro");
        writer.write(catalogFile, out);
        CatalogFile.SimpleReader reader = new CatalogFile.SimpleReader();
        CatalogFile readCatalogFile = reader.read(out.toInputFile());
        for (int i = 0; i < catalogFile.tables().size(); i++) {
            assert catalogFile.tables().get(i).equals(readCatalogFile.tables().get(i));
        }
    }
}
