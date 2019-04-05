package re.usto.smoque.queue;

/**
 * Created by tjamir on 10/4/17.
 */
public interface SmoqueConsumer {
    void onMessage(String producer, byte[] payload);


}
