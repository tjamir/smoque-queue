package re.usto.smoque.queue;

/**
 * Created by tjamir on 9/27/17.
 */
public class BadFormatException extends Exception{

    private String field;

    public String getField() {
        return field;
    }

    public BadFormatException setField(String field) {
        this.field = field;
        return this;
    }


    public BadFormatException(String s, String field) {
        super(s);
        this.field = field;
    }
}
