package re.usto.smoque.queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import re.usto.smoque.SmoqueConfig;
import re.usto.smoque.SmoqueWire;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by tjamir on 9/20/17.
 */
public class SmoqueTailer {

    private SmoqueQueue smoqueQueue;

    private SmoqueConfig smoqueConfig;

    private QueuePoller poller;

    protected long currentPosition;

    private Set<SmoqueConsumer> consumers;

    private Logger logger = LogManager.getLogger(SmoqueTailer.class);


    public SmoqueTailer() {

        this.consumers = new CopyOnWriteArraySet<>();
    }


    protected SmoqueTailer setPoller(QueuePoller poller) {
        this.poller = poller;
        return this;
    }

    protected SmoqueTailer setSmoqueQueue(SmoqueQueue smoqueQueue) {
        this.smoqueQueue = smoqueQueue;
        this.smoqueConfig = smoqueQueue.getSmoqueConfig();
        return this;
    }

    public void start() throws IOException {
        QueueHeader queueHeader = smoqueQueue.getQueueHeader();
        this.currentPosition = queueHeader.getNextMessagePosition();
        poller.setTailer(this);
        poller.setQueue(smoqueQueue);
        poller.start(queueHeader);
    }


    protected void onAppend(QueueHeader queueHeader) throws IOException {
        long lastWrittenPosition = queueHeader.getNextMessagePosition();
        boolean loop = currentPosition > lastWrittenPosition;
        logger.debug("Content append. Queue header " + queueHeader+" Current position "+currentPosition);

        while (this.currentPosition < queueHeader.getNextMessagePosition()|| loop) {
            try {
                long magicNumber = smoqueQueue.readMagicNumber(currentPosition);
                if (SmoqueWire.RING_RESET_MAGIC_NUMBER == magicNumber) {
                    logger.debug("Reseting queue position on " + smoqueQueue.getQueueId());
                    currentPosition = (long) QueueHeader.LENGTH;
                    loop = false;
                } else if(SmoqueWire.MAGIC_NUMBER == magicNumber){
                    logger.debug("Reading from postion " + currentPosition);
                    SmoqueWire smoqueWire = smoqueQueue.readWire(currentPosition);
                    currentPosition += smoqueWire.getTotalSize();
                    consumers.parallelStream().forEach(consumer ->
                            this.notifyConsumer(smoqueWire, consumer)
                    );
                } else {
                    logger.error("Tailer lost. Attempting to skip existing messages");
                    this.currentPosition = queueHeader.getNextMessagePosition();
                }
            } catch (BadFormatException e) {
                //TODO: Nesse caso tera que varrer atras de um proximo magic
                // number. Por enquanto, vamos ler novas mensagens
                e.printStackTrace();
                logger.error("Tailer lost. Attempting to skip existing messages");
                this.currentPosition = queueHeader.getNextMessagePosition();
                return;
            }
        }
    }

    private void notifyConsumer(SmoqueWire smoqueWire, SmoqueConsumer consumer) {
        try {
            byte[] payload = smoqueWire.getPayLoad();
            byte[] producerIdBytes = smoqueWire.getProducerId();
            String producer = new String(producerIdBytes, "utf-8");
            consumer.onMessage(producer, payload);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


    public void addConsumer(SmoqueConsumer smoqueConsumer) {
        consumers.add(smoqueConsumer);

    }


    public void stop() {
        this.poller.stop();
    }

    public boolean removeConsumer(SmoqueConsumer consumer) {

        return consumers.remove(consumer);
    }
}
