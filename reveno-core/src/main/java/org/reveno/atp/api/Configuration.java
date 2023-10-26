package org.reveno.atp.api;

public interface Configuration {

    SnapshotConfiguration snapshotting();

    DisruptorConfiguration disruptor();

    JournalingConfiguration journaling();


    Configuration mutableModel();

    Configuration immutableModel();

    Configuration mutableModelFailover(MutableModelFailover mutableModelFailover);

    Configuration cpuConsumption(CpuConsumption cpuConsumption);

    void mapCapacity(int capacity);

    void mapLoadFactor(float loadFactor);

    default void modelType(ModelType modelType) {
        switch (modelType) {
            case MUTABLE:
                mutableModel();
                break;
            case IMMUTABLE:
                immutableModel();
                break;
        }
    }

    enum ModelType {MUTABLE, IMMUTABLE} //可变的, 不可改变的

    enum MutableModelFailover {SNAPSHOTS, COMPENSATING_ACTIONS}

    enum CpuConsumption {LOW, NORMAL, HIGH, PHASED}

    interface SnapshotConfiguration {
        SnapshotConfiguration atShutdown(boolean takeSnapshot);

        SnapshotConfiguration every(long transactionCount);

        SnapshotConfiguration interval(long millis);
    }

    interface DisruptorConfiguration {
        DisruptorConfiguration bufferSize(int bufferSize);
    }


    interface JournalingConfiguration {
        JournalingConfiguration maxObjectSize(int size);

        JournalingConfiguration volumesSize(long txSize, long eventsSize);

        JournalingConfiguration volumes(int volumes);

        JournalingConfiguration minVolumes(int volumes);

        JournalingConfiguration channelOptions(ChannelOptions options);
    }

}
