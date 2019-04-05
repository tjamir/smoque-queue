package re.usto.smoque.queue;

/**
 * Created by tjamir on 9/14/17.
 */
public interface SmoqueProducer {

    public void consume(String producerId, byte[] payload);

}
