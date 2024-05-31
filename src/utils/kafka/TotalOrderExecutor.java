package utils.kafka;

import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/*
 *
 * This example shows how to use Kafka to order messages representating
 * operations and executing them in total order in different threads.
 *
 * For a real application, the operation would need to encode to actual operation and its arguments.
 * Instead of threads, different machines would execute the operation in the same order (total order)
 * and thus achieve state machine replication supported by Kafka's group communication support.
 *
 */
public class TotalOrderExecutor extends Thread implements RecordProcessor {
    static final String FROM_BEGINNING = "earliest";
    static final String TOPIC = "single_partition_topic";
    static final String KAFKA_BROKERS = "kafka:9092";

    static int MAX_NUM_THREADS = 4;

    final String replicaId;
    final KafkaPublisher sender;
    final KafkaSubscriber receiver;
    final SyncPoint<String> sync;

    TotalOrderExecutor( String replicaId ) {
        this.replicaId = replicaId;
        this.sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
        this.receiver.start(false, this);
        this.sync = new SyncPoint<>();
    }


    public void run() {
        for(;;) {
            var operation = "op-" + UUID.randomUUID();
            var version = sender.publish(TOPIC, replicaId, operation);
            var result = sync.waitForResult(version);
            System.out.printf("Op: %s, version: %s, result: %s\n", operation, version, result);
            sleep(500);
            //System.err.printf("replicaId: %s, sync state: %s", replicaId, sync);
        }
    }

    @Override
    public void onReceive(ConsumerRecord<String, String> r) {
        var version = r.offset();
        System.out.printf("%s : processing: (%d, %s)\n", replicaId, version, r.value());

        var result = "result of " + r.value();
        sync.setResult( version, result);
    }

    private void sleep( int ms ) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args ) throws Exception {

        KafkaUtils.createTopic(TOPIC, 1, 1);

        for( int i = 0; i < MAX_NUM_THREADS; i++)
            new TotalOrderExecutor( "replica(" + i + ")").start();
    }
}
