package org.reveno.atp.core.engine.components;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.reveno.atp.api.Configuration;
import org.reveno.atp.api.RepositorySnapshotter;
import org.reveno.atp.api.RepositorySnapshotter.SnapshotIdentifier;
import org.reveno.atp.api.domain.RepositoryData;
import org.reveno.atp.api.domain.WriteableRepository;
import org.reveno.atp.api.transaction.TransactionInterceptor;
import org.reveno.atp.api.transaction.TransactionStage;
import org.reveno.atp.core.JournalsManager;
import org.reveno.atp.core.RevenoConfiguration;
import org.reveno.atp.core.api.SystemInfo;
import org.reveno.atp.core.api.storage.JournalsStorage;
import org.reveno.atp.core.api.storage.SnapshotStorage;
import org.reveno.atp.core.snapshots.SnapshottersManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

public class SnapshottingInterceptor implements TransactionInterceptor {
	protected static final Logger LOG = LoggerFactory.getLogger(SnapshottingInterceptor.class);
	public static final long SNAPSHOTTING_FLAG = 0x345;
	protected RevenoConfiguration configuration;
	protected SnapshottersManager snapshotsManager;
	protected JournalsStorage journalsStorage;
	protected SnapshotStorage snapshotStorage;
	protected JournalsManager journalsManager;
	protected final ExecutorService executor = Executors.newSingleThreadExecutor();
	protected long counter = 1L;
	protected NonBlockingHashMapLong<SnapshotIdentifier[]> snapshots = new NonBlockingHashMapLong<>();
	protected NonBlockingHashMapLong<Future<?>> futures = new NonBlockingHashMapLong<>();

	@Override
	public void intercept(long transactionId, long time, long systemFlag, WriteableRepository repository, TransactionStage stage) {
		if (stage == TransactionStage.TRANSACTION) {
			if ((systemFlag & SNAPSHOTTING_FLAG) == SNAPSHOTTING_FLAG ||
					(configuration.revenoSnapshotting().every() > 0 && counter++ % configuration.revenoSnapshotting().every() == 0)) {
				asyncSnapshot(repository.getData(), transactionId);
				if (configuration.modelType() == Configuration.ModelType.MUTABLE) {
					try {
						futures.remove(transactionId).get();
					} catch (InterruptedException | ExecutionException ignored) {
					}
				}
			}
		} else if (stage == TransactionStage.JOURNALING && snapshots.containsKey(transactionId)) {
			if (configuration.modelType() != Configuration.ModelType.MUTABLE) {
				try {
					futures.remove(transactionId).get();
				} catch (InterruptedException | ExecutionException e) {
					return;
				}
			}
			try {
				SnapshotIdentifier[] ids = snapshots.remove(transactionId);
				final List<RepositorySnapshotter> snaps = snapshotsManager.getAll();
				final long lastJournalVersion = journalsStorage.getLastStoreVersion();
				for (int i = 0; i < ids.length; i++) {
					snaps.get(i).commit(lastJournalVersion, ids[i]);
				}
			} finally {
				journalsManager.roll(transactionId);
			}
		}
	}
	
	@Override
	public void destroy() {
		if (!executor.isShutdown()) {
			executor.shutdown();
			try {
				executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	private void asyncSnapshot(RepositoryData data, long transactionId) {
		data.getData().computeIfAbsent(SystemInfo.class, k -> new HashMap<>()).put(0L, new SystemInfo(transactionId));
		final List<RepositorySnapshotter> snaps = snapshotsManager.getAll();
		final SnapshotIdentifier[] ids = new SnapshotIdentifier[snaps.size()];

		for (int i = 0; i < snaps.size(); i++) {
			ids[i] = snaps.get(i).prepare();
		}
		futures.put(transactionId, executor.submit(() -> {
			for (int i = 0; i < snaps.size(); i++) {
				try {
					snaps.get(i).snapshot(data, ids[i]);
				} catch (Throwable t) {
					LOG.error(t.getMessage(), t);
				}
			}
		}));
		snapshots.put(transactionId, ids);
	}
	
	public SnapshottingInterceptor(RevenoConfiguration configuration,
								   SnapshottersManager snapshotsManager, SnapshotStorage snapshotStorage,
								   JournalsStorage journalsStorage, JournalsManager journalsManager) {
		this.configuration = configuration;
		this.snapshotsManager = snapshotsManager;
		this.journalsManager = journalsManager;
		this.journalsStorage = journalsStorage;
		this.snapshotStorage = snapshotStorage;
	}

}
