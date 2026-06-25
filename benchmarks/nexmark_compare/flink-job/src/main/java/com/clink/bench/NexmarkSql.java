package com.clink.bench;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Runs a Nexmark query on Flink SQL for the cross-engine comparison. Reads a .sql
 * file (CREATE TABLE source + sink, then INSERT INTO ... SELECT), splits it into
 * statements, and executes each via TableEnvironment so the same query text both
 * engines run can be kept in one place. The Kafka SQL connector is shaded into
 * this jar (it is not in the image); the table planner / json format are provided
 * by the image.
 *
 *   flink run -d nexmark-sql.jar /path/in/container/qN.sql
 *
 * The statements are split on ';'; the Nexmark SQL used here has no ';' inside a
 * statement. The final INSERT submits the job (detached under `flink run -d`).
 */
public final class NexmarkSql {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: NexmarkSql <sql-file>");
            System.exit(2);
        }
        String sql = Files.readString(Paths.get(args[0]));
        // Strip whole-line "--" comments first, so a leading comment block does
        // not get glued onto (and hide) the statement that follows it.
        StringBuilder cleaned = new StringBuilder();
        for (String line : sql.split("\n")) {
            if (line.trim().startsWith("--")) {
                continue;
            }
            cleaned.append(line).append("\n");
        }
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        for (String raw : cleaned.toString().split(";")) {
            String stmt = raw.trim();
            if (stmt.isEmpty()) {
                continue;
            }
            tEnv.executeSql(stmt);
        }
    }
}
