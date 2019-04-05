package re.usto.smoque.io;

import org.junit.After;
import org.junit.Test;
import re.usto.smoque.util.UnsafeFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * Created by tjamir on 9/18/17.
 */
public class MapperTest {


    private final static Path baseFile= Paths.get("test.bin");
    @Test
    public void map() throws IOException {
        Mapper mapper=new Mapper();
        long position=mapper.map(baseFile, 5L);
        byte[] data=new byte[4096];
        byte[] data2="Hello World!".getBytes();
        System.arraycopy(data2, 0, data, 0, data2.length);
        assertTrue(position>0);
        UnsafeFactory.getUnsafe().putInt(position, 0);
        UnsafeFactory.getUnsafe().copyMemory(data, Mapper.BYTE_ARRAY_OFFSET, null, position, data.length);
        assertTrue(Files.size(baseFile)==4096);
        mapper.unMap(position, 5L);
    }

    @After
    public void tearDown() throws IOException {
        Files.delete(baseFile);
    }




}