package tech.ydb.examples.simple;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;


/**
 * @author Sergey Polovko
 */
public class SchemeQuery extends SimpleExample {

    @Override
    protected void run(GrpcTransport transport, String pathPrefix) {
        String tablePath = pathPrefix + getClass().getSimpleName();
        TableClient tableClient = TableClient.newClient(transport).build();

        Session session = tableClient.createSession()
            .join()
            .expect("cannot create session");

        String createTable =
            "CREATE TABLE [" + tablePath + "] (" +
            "  key Uint32," +
            "  value String," +
            "  PRIMARY KEY(key)" +
            ");";

        session.executeSchemeQuery(createTable)
            .join()
            .expect("cannot create table");

        session.executeSchemeQuery("DROP TABLE [" + tablePath + "];")
            .join()
            .expect("cannot drop table");

        session.close()
            .join()
            .expect("cannot close session");
    }

    public static void main(String[] args) {
        new SchemeQuery().doMain();
    }
}
