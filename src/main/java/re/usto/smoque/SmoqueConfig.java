package re.usto.smoque;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by tjamir on 9/20/17.
 */
public class SmoqueConfig {


    private Path path;

    private long pollingIntervalNanos;

    private long acquireIntervalNanos;

    private long acquireTimeoutNanos;

    private long messageTimeoutMs;


    public SmoqueConfig(){
        this.path = Paths.get(".");
        pollingIntervalNanos=100;
        acquireIntervalNanos=20;
        acquireTimeoutNanos =0L;
        messageTimeoutMs =5000L;
    }


    public Path getPath() {
        return path;
    }

    public SmoqueConfig setPath(Path path) {
        this.path = path;
        return this;
    }

    public long getPollingIntervalNanos() {
        return pollingIntervalNanos;
    }

    public SmoqueConfig setPollingIntervalNanos(long pollingIntervalNanos) {
        this.pollingIntervalNanos = pollingIntervalNanos;
        return this;
    }

    public long getAcquireIntervalNanos() {
        return acquireIntervalNanos;
    }

    public SmoqueConfig setAcquireIntervalNanos(long acquireIntervalNanos) {
        this.acquireIntervalNanos = acquireIntervalNanos;
        return this;
    }

    public long getAcquireTimeoutNanos() {
        return acquireTimeoutNanos;
    }

    public SmoqueConfig setAcquireTimeoutNanos(long acquireTimeoutNanos) {
        this.acquireTimeoutNanos = acquireTimeoutNanos;
        return this;
    }

    public long getMessageTimeoutMs() {
        return messageTimeoutMs;
    }

    public void setMessageTimeoutMs(long messageTimeoutMs) {
        this.messageTimeoutMs = messageTimeoutMs;
    }
}
