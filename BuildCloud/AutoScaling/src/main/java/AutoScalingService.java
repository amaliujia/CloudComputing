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
import java.util.*;

/**
 * Created by amaliujia on 16-2-8.
 */
public class AutoScalingService {
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

        //this.authenticate();
        System.out.println("authentication succeed!");

        this.initHttpServer();
        this.initCloudWatch();
    }

    private void initHttpServer() {
        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1024));

        // Create http server
        HttpServerOptions serverOptions = new HttpServerOptions();
        httpServer = vertx.createHttpServer(serverOptions);

        // Create Router
        router = Router.router(vertx);
        router.route("/").handler(routingContext -> {
            routingContext.response().end("OK");
        });

        router.route("/stop").handler(AutoScalingService::handleStop);

        // Listen for the request on port 8080
        httpServer.requestHandler(router::accept).listen(8080);
    }

    private void initCloudWatch() {
        AutoScalingService.instances = new ArrayList<>();
        AutoScalingService.flagWrapper = new FlagWrapper();
        AutoScalingService.flagWrapper.isShutdown = false;

        String min_instance_str = AutoScalingService.params.get("MIN_INSTANCE");
        int min_instance = Integer.parseInt(min_instance_str);
        String flavorId = AutoScalingService.params.get("DC_FLAVOR");
        String imageId = AutoScalingService.params.get("DC_IMAGE");
        String LB_IP = AutoScalingService.params.get("LB_IPADDR");

        for (int i = 0; i < min_instance; i++) {
            // Create a Server Model Object
            ServerCreate VMInstance = Builders.server().name("DC-" + i).flavor(flavorId).image(imageId).build();

            // Boot the Server
            Server server = os.compute().servers().boot(VMInstance);

            // add instance to index
            DataCenterInstance instance = new DataCenterInstance(server);
            HttpConnections.addInstanceToLoadBalancer(LB_IP, instance);
            AutoScalingService.instances.add(instance);
        }

        try {
            Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread cloudwater = new Thread() {
            public void run() {
                CloudWatch watch = new CloudWatch(AutoScalingService.params,
                                                    AutoScalingService.os,
                                                    AutoScalingService.instances,
                                                    AutoScalingService.flagWrapper);
                watch.start();
            }
        };

        cloudwater.start();

    }

    private static void handleStop(RoutingContext routingContext) {
        System.out.println("\nAuto Scaling Group will shut down.\n");
        AutoScalingService.flagWrapper.isShutdown = true;

        // ready to remove all instances
        Iterator<DataCenterInstance> iterator = instances.iterator();
        while (iterator.hasNext()) {
            DataCenterInstance instance = iterator.next();

            System.out.println("\nShutting down....");
            System.out.println(instance.toString());
            // stop instance in Nova
            os.compute().servers().action(instance.getServerID(), Action.STOP);

            // remove from LB
            HttpConnections.removeInstanceFromLoadBalancer(AutoScalingService.params.get("LB_IPADDR"), instance);

            // remove from index
            iterator.remove();
            System.out.println("Shut down....\n");

        }
    }

    private void authenticate() {
        String username = AutoScalingService.params.get("USER_NAME");
        String password = AutoScalingService.params.get("PASSWORD");
        os = OSFactory.builder()
                .endpoint("http://54.164.97.164:5000/v2.0")
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

//import urllib
//
//        myPort = "8080"
//        myParameters = { "date" : "whatever", "another_parameters" : "more_whatever" }
//
//        myURL = "http://localhost:%s/read?%s" % (myPort, urllib.urlencode(myParameters))
