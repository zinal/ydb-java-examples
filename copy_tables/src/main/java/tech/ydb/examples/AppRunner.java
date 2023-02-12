package tech.ydb.examples;

import java.nio.file.Files;
import java.nio.file.Paths;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.nio.file.Path;
import tech.ydb.auth.iam.CloudAuthIdentity;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;


/**
 * @author Sergey Polovko
 */
public class AppRunner {
    private AppRunner() { }

    private static final class Args {
        @Parameter(names = { "-e", "--endpoint" }, description = "YDB endpoint", required = true, help = true)
        String endpoint;

        @Parameter(names = { "-d", "--database" }, description = "YDB database name", required = true, help = true)
        String database;

        @Parameter(names = { "-p", "--path" }, description = "Base path for tables", help = true)
        String path;

        @Parameter(names = { "-c", "--cert" }, description = "Path to PEM certificate", help = true)
        String certPath;
    }

    public static void run(String appName, App.Factory appFactory, String... params) {
        System.exit(safeRun(appName, appFactory, params));
    }

    public static int safeRun(String appName, App.Factory appFactory, String... params) {
        Args args = new Args();
        JCommander jc = JCommander.newBuilder()
            .addObject(args)
            .build();
        jc.setProgramName(appName);

        try {
            jc.parse(params);
        } catch (Exception e) {
            jc.usage();
            return 1;
        }

        GrpcTransportBuilder transportBuilder = GrpcTransport.forEndpoint(args.endpoint, args.database);

        String ydbToken = System.getenv("YDB_TOKEN");

        if (ydbToken != null && !ydbToken.isEmpty()) {
            transportBuilder = transportBuilder.withAuthProvider(new TokenAuthProvider(ydbToken));
        } else {
            ydbToken = System.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            if (ydbToken != null && !ydbToken.isEmpty()) {
                final Path p =  Paths.get(ydbToken);
                transportBuilder = transportBuilder.withAuthProvider(opt ->
                        CloudAuthIdentity.serviceAccountIdentity(p));
            }
        }

        if (args.certPath != null) {
            try {
                transportBuilder.withSecureConnection(Files.readAllBytes(Paths.get(args.certPath)));
            } catch (Exception e) {
                System.err.println("Cannot read certificate from file " + args.certPath + ": " + e.getMessage());
                return 1;
            }
        }

        try (GrpcTransport transport = transportBuilder.build()) {
            String path = args.path == null ? args.database : args.path;
            try (App example = appFactory.newApp(transport, path)) {
                example.run();
            } catch (Throwable t) {
                t.printStackTrace();
                return 1;
            }
        } catch (Throwable t) {
            t.printStackTrace();
            return 1;
        }

        return 0;
    }
}
