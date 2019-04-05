package re.usto.smoque;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import re.usto.smoque.queue.SmoqueQueue;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.*;

/**
 * Created by tjamir on 10/16/17.
 */
public class SmoqueTest {
    public static final long TWO_GIGABYTES = 2L * 1024 * 1024 * 1024;
    public static final Path TESTFOLDER = Paths.get("testfolder");
    private Smoque smoque;

    private SmoqueConfig smoqueConfig;

    @Before
    public void setUp() throws Exception {
        smoqueConfig=new SmoqueConfig().setPath(TESTFOLDER);
        smoque = new Smoque(smoqueConfig);
        Files.createDirectories(TESTFOLDER);
    }

    @Test

    public void getProcessId(){
        assertTrue(smoque.getProcessId()>0);
    }


    @Test
    public void getQueue() throws Exception {
        SmoqueQueue smoqueQueue=smoque.getQueue("queue1", TWO_GIGABYTES);
        assertNotNull(smoqueQueue);
        assertEquals(smoqueQueue.getSmoqueConfig(), smoqueConfig);
        assertEquals(smoque.getProcessId(), smoqueQueue.getProcessId());
        SmoqueQueue anotherQueue=smoque.getQueue("queue2", TWO_GIGABYTES);
        assertNotEquals(smoqueQueue, anotherQueue);
        assertEquals(smoque.getProcessId(), anotherQueue.getProcessId());
        SmoqueQueue sameQueue=smoque.getQueue("queue1", TWO_GIGABYTES);
        assertEquals(smoqueQueue, sameQueue);

    }

    @After
    public void tearDown() throws IOException {
        Files.walkFileTree(TESTFOLDER, new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                Files.delete(path);
                return super.visitFile(path, basicFileAttributes);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                Files.delete(path);
                return super.postVisitDirectory(path, e);
            }
        });

    }

}