package re.usto.smoque.util;

import org.junit.Test;
import sun.misc.Unsafe;

import static org.junit.Assert.*;

/**
 * Created by tjamir on 9/14/17.
 */
public class UnsafeFactoryTest {

    @Test
    public void getUnsafe() throws NoSuchFieldException, IllegalAccessException {

        Unsafe unsafe = UnsafeFactory.getUnsafe();
        assertNotNull(unsafe);
        assertTrue(unsafe==UnsafeFactory.getUnsafe());

    }

}