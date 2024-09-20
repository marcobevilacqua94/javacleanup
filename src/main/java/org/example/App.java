package org.example;
import com.couchbase.client.java.*;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupEndRunEvent;
import com.couchbase.client.java.transactions.TransactionKeyspace;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;

import java.time.Duration;
import java.util.List;


/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) {
        try (Cluster cluster = Cluster.connect(
                args[0],
                ClusterOptions.clusterOptions(args[1], args[2])
                        .environment(env -> env.transactionsConfig(TransactionsConfig
                           //     .metadataCollection(TransactionKeyspace.create("test", "test", "test"))
                           //     .durabilityLevel(DurabilityLevel.NONE)
                                .timeout(Duration.ofSeconds(20))
                                .cleanupConfig(
                                        TransactionsCleanupConfig
                                                .cleanupClientAttempts(true)
                                                .cleanupLostAttempts(true)
                                                .cleanupWindow(Duration.ofSeconds(10))
                                                .addCollections(List.of(
                                                        TransactionKeyspace.create("test", "test", "test"),
                                                        TransactionKeyspace.create("test", "test", "warmup")
                                                )))))
        )

        ) {
            cluster.environment().eventBus().subscribe(event -> {
                if (event instanceof TransactionCleanupAttemptEvent || event instanceof TransactionCleanupEndRunEvent) {
                    System.out.println(event.description());
                }
            });
            Thread.sleep(9999999999999L);


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
