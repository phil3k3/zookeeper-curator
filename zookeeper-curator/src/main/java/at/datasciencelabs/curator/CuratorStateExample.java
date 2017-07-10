package at.datasciencelabs.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.nio.file.Paths;

/**
 * Demonstrates the use of Apache Curator to read and write state information from and to Zookeeper
 */
class CuratorStateExample {

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new RetryOneTime(1000);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
        client.start();

        // write entries for AH idnr 300 with state ERROR
        IdempotentCuratorState ruleState300 = new IdempotentCuratorState(Paths.get("agent", "rule", "300"), RuleState.ERROR.name().getBytes());
        ruleState300.write(client);

        // write entries for AH idnr 200 with state RUNNING
        IdempotentCuratorState ruleState200 = new IdempotentCuratorState(Paths.get("agent", "rule", "200"), RuleState.RUNNING.name().getBytes());

        ruleState200.write(client);

        printRuleState(client, ruleState300);
        printRuleState(client, ruleState200);
    }

    private static void printRuleState(CuratorFramework client, IdempotentCuratorState idempotentCuratorState) throws Exception {
        byte[] bytes = client.getData().forPath(idempotentCuratorState.getPath());
        RuleState ruleState = RuleState.valueOf(new String(bytes));
        System.out.println("Rule State " + ruleState.name());
    }
}
