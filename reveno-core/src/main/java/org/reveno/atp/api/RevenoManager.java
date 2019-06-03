package org.reveno.atp.api;

import org.reveno.atp.api.commands.CommandContext;
import org.reveno.atp.api.domain.Repository;
import org.reveno.atp.api.dynamic.AbstractDynamicTransaction;
import org.reveno.atp.api.dynamic.DirectTransactionBuilder;
import org.reveno.atp.api.query.ViewsMapper;
import org.reveno.atp.api.transaction.TransactionContext;
import org.reveno.atp.core.api.serialization.TransactionInfoSerializer;
import org.reveno.atp.core.api.storage.FoldersStorage;
import org.reveno.atp.core.api.storage.SnapshotStorage;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Contains all required operations for managing of domain space of engine.
 * Basically it includes registration of commands and transaction actions along
 * with their handlers. You can put here your own View mappers as well, which
 * naturally are the bridge between Query side and your whole domain model.
 * <p>
 * Also you can provide your own serializers chain or domain snapshotters as well.
 *
 * @author Artem Dmitriev <art.dm.ser@gmail.com>
 */
public interface RevenoManager {

    /**
     * TODO
     *
     * @param name
     * @param handler
     * @return
     */
    DirectTransactionBuilder transaction(String name, BiConsumer<AbstractDynamicTransaction, TransactionContext> handler);

    /**
     * TODO
     *
     * @param transaction
     * @param handler
     */
    <T> void transactionAction(Class<T> transaction, BiConsumer<T, TransactionContext> handler);

    /**
     * TODO
     *
     * @param transaction
     * @param handler
     */
    <T> void transactionWithCompensatingAction(Class<T> transaction, BiConsumer<T, TransactionContext> handler,
                                               BiConsumer<T, TransactionContext> compensatingAction);

    /**
     * TODO
     *
     * @param commandType
     * @param resultType
     * @param handler
     */
    <C, U> void command(Class<C> commandType, Class<U> resultType, BiFunction<C, CommandContext, U> handler);

    /**
     * TODO
     *
     * @param commandType
     * @param handler
     */
    <C> void command(Class<C> commandType, BiConsumer<C, CommandContext> handler);

    /**
     * TODO
     *
     * @param entityType
     * @param viewType
     * @param mapper
     */
    <E, V> void viewMapper(Class<E> entityType, Class<V> viewType, ViewsMapper<E, V> mapper);

    /**
     * Specifies some {@link org.reveno.atp.api.RepositorySnapshotter} object which is responsible
     * for creating some snapshot of current state of domain model from {@link Repository}, and storing
     * it with either {@link SnapshotStorage} or creating custom storage structure with {@link FoldersStorage}.
     * <p>
     * You can provide multiple snapshotters using this method, and they will be called in chain on every
     * snapshot operation initiated by the engine.
     *
     * @param snapshotter to be added in chain.
     * @return
     */
    RevenoManager snapshotWith(RepositorySnapshotter snapshotter);

    /**
     * Despite the fact that you can provide multiple snapshotters, for now you can use only one for
     * restoration of domain model state, which should be specified by calling that method.
     *
     * @param snapshotter to be used for creation of snapshots.
     */
    void restoreWith(RepositorySnapshotter snapshotter);

    /**
     * This method does the same as {@link #restoreWith(RepositorySnapshotter)}, but uses the last
     * snapshotter, which was provided by {@link #snapshotWith(RepositorySnapshotter)} method call.
     */
    void andRestoreWithIt();

    /**
     * Removes all snapshotters from chain.
     */
    void resetSnapshotters();

    /**
     * Allows to provide custom {@link TransactionInfoSerializer} chain, with which all the transactions
     * states will be serialized and journaled.
     * <p>
     * When serialization performs, it starts with first serializer in the list. If the serialization was
     * successful, then the it doesn't use the rest. Otherwise, it will start trying with next serializers
     * until the first success. So, it make sense to put serializers in list starting with most effective and
     * probably leaving less effective but the most durable in the end, to prevent cases where all fails.
     *
     * @param serializers to be used for serialization of transaction info.
     */
    void serializeWith(List<TransactionInfoSerializer> serializers);


    default RevenoManager and(RepositorySnapshotter snapshotter) {
        Objects.requireNonNull(snapshotter);

        snapshotWith(snapshotter);
        return this;
    }

}
