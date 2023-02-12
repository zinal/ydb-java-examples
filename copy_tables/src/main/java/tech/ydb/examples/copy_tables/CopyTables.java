package tech.ydb.examples.copy_tables;

import java.util.ArrayList;

import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.examples.App;
import tech.ydb.examples.AppRunner;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.CopyTablesSettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;

/**
 * This example creates 3 tables, fills those tables with test records,
 * and creates a snapshot-style copy of all those tables.
 * @author zinal
 */
public class CopyTables implements App {
    private static final String TABLE1_NAME = "table1";
    private static final String TABLE2_NAME = "table2";
    private static final String TABLE3_NAME = "table3";

    private final String databasePath;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;

    CopyTables(GrpcTransport transport, String databasePath) {
        this.databasePath = databasePath;
        this.tableClient = TableClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    @Override
    public void run() {
        createTables();
        insertData();
        copyTables();
        dropTables();
    }

    private void createTable(String tableName, TableDescription tableDesc) {
        Status status = retryCtx.supplyStatus(session ->
                session.createTable(databasePath + "/" + tableName, tableDesc)).join();
        if (status != Status.SUCCESS) {
            System.out.println(String.format("Table %s creation failed with status: %s", tableName, status));
        } else {
            System.out.println(String.format("Table %s created", tableName));
        }
    }

    private void createTables() {
        TableDescription tableDesc = TableDescription.newBuilder()
                .addNullableColumn("a", PrimitiveType.Uint64)
                .addNullableColumn("b", PrimitiveType.Uint64)
                .addNullableColumn("c", PrimitiveType.Text)
                .addNullableColumn("d", PrimitiveType.Text)
                .setPrimaryKeys("a", "b")
                .build();
        createTable(TABLE1_NAME, tableDesc);
        createTable(TABLE2_NAME, tableDesc);
        createTable(TABLE3_NAME, tableDesc);
    }

    private void dropTable(String tableName) {
        Status status = retryCtx.supplyStatus(session ->
                session.dropTable(databasePath + "/" + tableName)).join();
        if (status != Status.SUCCESS) {
            System.out.println(String.format("Table %s removal failed with status: %s", tableName, status));
        } else {
            System.out.println(String.format("Table %s dropped", tableName));
        }
    }

    private void dropTables() {
        dropTable(TABLE1_NAME);
        dropTable(TABLE2_NAME);
        dropTable(TABLE3_NAME);
        dropTable(TABLE1_NAME + "_copy");
        dropTable(TABLE2_NAME + "_copy");
        dropTable(TABLE3_NAME + "_copy");
    }

    private void executeBatch(String query, ArrayList<Value<?>> pack) {
        Value<?>[] values = new Value<?>[pack.size()];
        pack.toArray(values);

        Params params = Params.of("$items", ListValue.of(values));

        TxControl txControl = TxControl.serializableRw().setCommitTx(true);
        retryCtx
                .supplyResult(session -> session.executeDataQuery(query, txControl, params))
                .join().getStatus().expectSuccess();
    }

    private void insertData(String tableName, int recordCount) {
        String query = String.format(
            "\n" +
            "DECLARE $items AS\n" +
            "List<Struct<\n" +
                "a: Uint64,\n" +
                "b: Uint64,\n" +
                "c: Utf8,\n" +
                "d: Utf8>>;\n" +
            "REPLACE INTO `%s`\n" +
            "SELECT * FROM AS_TABLE($items)\n", tableName);

        final Generator input = new Generator(recordCount);

        while (input.isValid()) {
            ArrayList<Value<?>> pack = new ArrayList<>();
            while (input.isValid() && pack.size() < 101) {
                pack.add(input.get());
            }
            executeBatch(query, pack);
        }
        System.out.println(String.format("%d records uploaded to %s", recordCount, tableName));
    }

    private void insertData() {
        insertData(TABLE1_NAME, 10);
        insertData(TABLE2_NAME, 100);
        insertData(TABLE3_NAME, 1000);
    }

    private void copyTables() {
        final CopyTablesSettings copyTables = new CopyTablesSettings();
        copyTables.addTable(TABLE1_NAME, TABLE1_NAME + "_copy");
        copyTables.addTable(TABLE2_NAME, TABLE2_NAME + "_copy");
        copyTables.addTable(TABLE3_NAME, TABLE3_NAME + "_copy");
        retryCtx.supplyStatus(session -> session.copyTables(copyTables)).join().expectSuccess();
    }

    @Override
    public void close() {
        tableClient.close();
    }

    public static int test(String[] args) {
        return AppRunner.safeRun("CopyTables", CopyTables::new, args);
    }

    public static void main(String[] args) {
        AppRunner.run("CopyTables", CopyTables::new, args);
    }

    static class Generator {
        private int remain;

        Generator(int count) {
            this.remain = count;
        }

        boolean isValid() {
            return remain > 0;
        }

        Value<?> get() {
            --remain;

            long urlNo = remain;
            long hostNo = urlNo / 10;

            String url = String.format("http://host-%d.ru:80/path_with_id/%d", hostNo, urlNo);
            String host = String.format("host-%d.ru:80", hostNo);

            long urlUid = url.hashCode();
            long hostUid = host.hashCode();

            String page = String.format("the page were page_num='%d' Url='%s' UrlUid='%d' HostUid='%d'",
                    remain, url, urlUid, hostUid);

            return StructValue.of(
                    "a", PrimitiveValue.newUint64(hostUid),
                    "b", PrimitiveValue.newUint64(urlUid),
                    "c", PrimitiveValue.newText(url),
                    "d", PrimitiveValue.newText(page)
            );
        }
    }
}
