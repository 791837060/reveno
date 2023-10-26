package org.reveno.atp.core.data;

import org.reveno.atp.core.api.Journaler;
import org.reveno.atp.core.api.channel.Buffer;
import org.reveno.atp.core.api.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class DefaultJournaler implements Journaler {
    protected static final Logger log = LoggerFactory.getLogger(DefaultJournaler.class);
    protected AtomicReference<Channel> channel = new AtomicReference<>();
    protected AtomicReference<Channel> oldChannel = new AtomicReference<>();
    protected volatile Runnable rolledHandler;
    protected volatile boolean isWriting = false;

    public String baseDir = "";

    @Override
    public void writeData(Consumer<Buffer> writer, boolean endOfBatch) {
        requireWriting();

        Channel ch = channel.get();
        ch.write(writer, endOfBatch);

        if (!oldChannel.compareAndSet(ch, ch)) {
            if (oldChannel.get().isOpen())
                closeSilently(oldChannel.get());
            oldChannel.set(ch);

            if (rolledHandler != null) {
                rolledHandler.run();
                rolledHandler = null;
            }
        }
    }

    @Override
    public void startWriting(Channel ch) {
        log.info("Started writing to {} {}", baseDir,ch);

        channel.set(ch);
        oldChannel.set(ch);
        isWriting = true;
    }

    @Override
    public void stopWriting() {
        log.info("Stopped writing to {} {}",baseDir, channel.get());

        isWriting = false;
        closeSilently(channel.get());
        if (oldChannel.get().isOpen())
            closeSilently(oldChannel.get());
    }

    @Override
    public Channel currentChannel() {
        return channel.get();
    }

    @Override
    public void roll(Channel ch, Runnable rolled) { //翻滚
        // Rolling to tmp_evn-2023_10_26-00000000000000000002-00000000000000000000
        // Rolling to tmp_tx-2023_10_26-00000000000000000002-00000000000000000000
        log.info("Rolling to {} {}", baseDir, ch);
        if (!isWriting) {
            startWriting(ch);
        }

        this.rolledHandler = rolled;
        channel.set(ch);
    }

    @Override
    public void destroy() {
        stopWriting();
    }


    protected void closeSilently(Channel ch) {
        try {
            ch.close();
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        }
    }

    protected void requireWriting() {
        if (!isWriting)
            throw new RuntimeException("Journaler must be in writing mode.");
    }

}
