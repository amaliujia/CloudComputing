import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.openstack4j.api.Builders;
import org.openstack4j.api.OSClient;
import org.openstack4j.model.compute.Action;
import org.openstack4j.model.compute.Server;
import org.openstack4j.model.compute.ServerCreate;
import org.openstack4j.openstack.OSFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by amaliujia on 16-2-8.
 */
public class AutoScalingService {
    private final int ASG_port=15066;

    private String config;

    private static Vertx vertx;
    private static HttpClient httpClient;
    private static HttpServer httpServer;
    private static Router router;

    public static Map<String, String> params;
    public static OSClient os;
    public static List<DataCenterInstance> instances;
    public static FlagWrapper flagWrapper;

    public static void main(String[] args) {
        AutoScalingService service = new AutoScalingService(args[0]);
        service.start();
    }

    public AutoScalingService(String config) {
        this.config = config;
    }

    public void start() {
        AutoScalingService.params = this.loadConfiguration(this.config);
        System.out.println(AutoScalingService.params.toString());

        this.authenticate();
        System.out.println("authentication succeed!");

        this.initBasic();
        this.initHttpServer();
        this.initCloudWatch();
    }

    private void initBasic() {
        AutoScalingService.instances = new ArrayList<>();
        AutoScalingService.flagWrapper = new FlagWrapper();
        AutoScalingService.flagWrapper.isShutdown = false;
    }

    private void initHttpServer() {
        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1024));

        // Create http server
        HttpServerOptions serverOptions = new HttpServerOptions();
        httpServer = vertx.createHttpServer(serverOptions);

        // Create Router
        router = Router.router(vertx);
        router.route("/").handler(routingContext -> {
            routingContext.response().setStatusCode(200).end("OK");
        });
        router.route("/add").handler(routingContext -> {
            routingContext.response().setStatusCode(200).end("OK");
        });
        router.route("/remove").handler(routingContext -> {
            routingContext.response().setStatusCode(200).end("OK");
        });

        router.route("/stop").handler(AutoScalingService::handleStop);

        // Listen for the request on port 15066
        httpServer.requestHandler(router::accept).listen(ASG_port);
    }

    private void initCloudWatch() {
        String min_instance_str = AutoScalingService.params.get("MIN_INSTANCE");
        int min_instance = Integer.parseInt(min_instance_str);
        String LB_IP = AutoScalingService.params.get("LB_IPADDR");

        for (int i = 0; i < min_instance; i++) {
            Server server = AutoScalingService.launchServer();

            // add instance to index
            DataCenterInstance instance = new DataCenterInstance(server);
            HttpConnections.addInstanceToLoadBalancer(LB_IP, instance);
            AutoScalingService.instances.add(instance);
        }

        Thread cloudwater = new Thread() {
            public void run() {
                CloudWatch watch = new CloudWatch(AutoScalingService.params,
                                                    AutoScalingService.instances,
                                                    AutoScalingService.flagWrapper);
                watch.start();
            }
        };
        cloudwater.start();
    }

    public static Server launchServer() {
        String flavorId = AutoScalingService.params.get("DC_FLAVOR");
        String imageId = AutoScalingService.params.get("DC_IMAGE");
        String name = AutoScalingService.params.get("ASG_NAME");
        String LB_IP = AutoScalingService.params.get("LB_IPADDR");
        // Create a Server Model Object

        // The Security Group has been built via dashboard
        ServerCreate VMInstance = Builders.server().name(name).flavor(flavorId).image(imageId).addSecurityGroup("719_all_traffic").build();

        // Boot the Server
        Server server = os.compute().servers().boot(VMInstance);
        System.out.println("Launch server " + server.getId() + ". Check status...\n");

        while (server.getStatus() != Server.Status.ACTIVE) {
            System.out.println("Build... ");
            server = os.compute().servers().get(server.getId());
            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Active...\n");

        while(true) {
            try {
                Thread.sleep(5 * 1000);
                URL obj = new URL("http://" + server.getAddresses().getAddresses().get("private").get(0).getAddr());
                HttpURLConnection con = (HttpURLConnection) obj.openConnection();
                con.setRequestMethod("GET");
                con.setRequestProperty("User-Agent", "Mozilla/5.0");
                con.setConnectTimeout(args.timeout);
                //con.setReadTimeout(args.timeout);

                int responseCode = con.getResponseCode();
                System.out.println("DC Response Code : " + responseCode);
                break;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                continue;
            }
        }

        return server;
    }

    public static void killServer(String id) {
        os.compute().servers().delete(id);
    }

    private static void handleStop(RoutingContext routingContext) {
        AutoScalingService.authenticate();

        System.out.println("\nAuto Scaling Group will shut down.\n");
        AutoScalingService.flagWrapper.isShutdown = true;
        String id_str = routingContext.request().getParam("id");
        int id = Integer.parseInt(id_str);

        // ready to remove all instances
        Iterator<DataCenterInstance> iterator = instances.iterator();
        while (iterator.hasNext()) {
            DataCenterInstance instance = iterator.next();
            System.out.println("\nShutting down " + instance.getIP() + " ...\n");
            HttpConnections.removeInstanceFromLoadBalancer(AutoScalingService.params.get("LB_IPADDR"), instance);

        }
        routingContext.response()
                        .setStatusCode(200)
                        .end("ASG " + id_str + " shutting down");
        AutoScalingService.httpServer.close();

        iterator = instances.iterator();
        while (iterator.hasNext()) {
            DataCenterInstance instance = iterator.next();
            // stop instance in Nova
            os.compute().servers().action(instance.getServerID(), Action.STOP);
            // remove from index
            // iterator.remove();
        }

        System.out.println("System down!");
        System.exit(0);
    }

    private static void authenticate() {
        String username = AutoScalingService.params.get("USER_NAME");
        String password = AutoScalingService.params.get("PASSWORD");
        String localAddr = null;
        try {

            localAddr = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        os = OSFactory.builder()
                .endpoint("http://" + localAddr +":5000/v2.0")
                .credentials(username, password)
                .tenantName(AutoScalingService.params.get("PROJECT_NAME"))
                .authenticate();
    }

    private Map<String, String> loadConfiguration(String filepath) {
        Map<String, String> ret = new HashMap<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(filepath)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("#")) {
                    String[] splits = line.split("=");
                    if (splits.length == 2) {
                        ret.put(splits[0], splits[1]);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("Cannot read configuration " + filepath);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Cannot read line from " + filepath);
            System.exit(1);
        }

        return ret;
    }
}