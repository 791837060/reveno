package org.reveno.atp.core.restore;

import org.reveno.atp.api.transaction.EventBus;
import org.reveno.atp.commons.LongRange;
import org.reveno.atp.core.api.EventsCommitInfo;
import org.reveno.atp.core.api.RestoreableEventBus;
import org.reveno.atp.core.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Special {@link EventBus} implementation, which is used on
 * restoration process. Basically, the workflow for this class
 * is following:
 * <p>
 * 1) During all events commit files processing, {@code processNextEvent}
 * method is called, noting that events for transaction N were processed.
 * Unless, new LongRange is added.
 * 2) During transaction replay, they publish events to {@code publishEvent}
 * as normally. If current transactionId is not belong to any LongRange, then
 * it is skipped, unless, it is being replayed as well.
 *
 * 1） 在所有事件提交文件处理期间，｛@code processNextEvent｝
 * 方法，注意事务N的事件已被处理。
 * 除非添加了新的LongRange。
 *
 * 2） 在事务重播期间，他们将事件发布到｛@code publishEvent｝
 * 正常情况下。如果当前transactionId不是任何LongRange的一部分，则
 * 它被跳过，除非它也在重播。
 */
public class RestorerEventBus implements RestoreableEventBus {
    protected static final Logger log = LoggerFactory.getLogger(RestorerEventBus.class);
    protected EventBus underlyingEventBus;
    protected long currentTransactionId = -1L;
    protected long lastTransactionId = -1L;
    protected long maxTransactionId = -1L;
    protected TreeSet<LongRange> unpublishedEvents = new TreeSet<>();

    @Override
    public void publishEvent(Object event) {
        if (currentTransactionId > maxTransactionId) {
            log.info("Current transaction id > max transaction id loaded from events. [{},{}]",
                    currentTransactionId, maxTransactionId);
            underlyingEventBus.publishEvent(event); //基础事件总线
            return;
        }
        Iterator<LongRange> i = unpublishedEvents.iterator();
        while (i.hasNext()) {
            LongRange range = i.next();
            if (!range.higher(currentTransactionId)) {
                if (range.contains(currentTransactionId)) {
                    underlyingEventBus.publishEvent(event); //基础事件总线
                }
                break;
            }
            i.remove();
        }
    }

    @Override
    public RestoreableEventBus currentTransactionId(long transactionId) {
        this.currentTransactionId = transactionId;
        return this;
    }

    @Override
    public RestoreableEventBus underlyingEventBus(EventBus eventBus) {
        this.underlyingEventBus = eventBus;
        return this;
    }

    public void processNextEvent(EventsCommitInfo event) {
        if (lastTransactionId == -1L) {
            lastTransactionId = event.transactionId();
            maxTransactionId = lastTransactionId;
            return;
        }

        if ((event.flag() & EventPublisher.ASYNC_ERROR_FLAG) == EventPublisher.ASYNC_ERROR_FLAG) {
            log.info("Failed transaction event [{}]", event.transactionId());
            unpublishedEvents.add(new LongRange(event.transactionId()));
            return;
        }
        if (event.transactionId() <= lastTransactionId && event.flag() == 0) {
            log.warn("Transaction ID < Last Transaction ID - this is abnormal [{};{}]", event.transactionId(), lastTransactionId);
            addMissedEvents(event);
        } else // TODO it might be just that not all transactions issue events, so nothing is missing
            // 可能只是不是所有事务都会发出事件，所以没有遗漏任何内容
            if (event.transactionId() - lastTransactionId > 1) {
                log.debug("Missing transaction events from {} to {}", lastTransactionId + 1, event.transactionId() - 1);
                unpublishedEvents.add(new LongRange(lastTransactionId + 1, event.transactionId() - 1));
            }
        lastTransactionId = event.transactionId();
        if (lastTransactionId > maxTransactionId) {
            maxTransactionId = lastTransactionId;
        }
    }

    protected void addMissedEvents(EventsCommitInfo event) {
        Set<LongRange> toAdd = new TreeSet<>();
        Iterator<LongRange> i = unpublishedEvents.iterator();
        while (i.hasNext()) {
            LongRange range = i.next();
            if (!range.higher(event.transactionId())) {
                if (range.contains(event.transactionId())) {
                    i.remove();
                    Collections.addAll(toAdd, range.split(event.transactionId()));
                    break;
                }
            }
            break;
        }
        unpublishedEvents.addAll(toAdd);
    }

    public Set<LongRange> getUnpublishedEvents() {
        return unpublishedEvents;
    }

    public void clear() {
        unpublishedEvents.clear();
    }
}
