package org.example;
import com.couchbase.client.java.*;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupEndRunEvent;
import com.couchbase.client.java.transactions.TransactionKeyspace;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;

import java.time.Duration;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) {
        try (Cluster cluster = Cluster.connect(
                args[0],
                ClusterOptions.clusterOptions(args[1], args[2])
                        .environment(env -> env.transactionsConfig(TransactionsConfig
                                .cleanupConfig(
                                        TransactionsCleanupConfig
                                                .cleanupWindow(Duration.ofSeconds(Integer.parseInt(args[3])))
                                                .addCollection(TransactionKeyspace.create("test"))
                                                )))
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
