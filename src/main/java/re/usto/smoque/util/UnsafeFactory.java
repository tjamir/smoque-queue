package re.usto.smoque.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by tjamir on 9/14/17.
 */
public class UnsafeFactory {

    private static Unsafe unsafe;

    public static synchronized Unsafe getUnsafe()  {
        try {
            if (unsafe == null) {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                unsafe = (Unsafe) theUnsafe.get(null);
            }
        }catch (Throwable t){
            throw new RuntimeException(t);
        }
        return unsafe;
    }
}
