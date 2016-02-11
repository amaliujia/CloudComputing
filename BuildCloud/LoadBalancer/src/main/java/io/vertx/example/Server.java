package io.vertx.example;

import com.google.common.net.InetAddresses;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.impl.StringEscapeUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import javax.naming.NamingException;
import javax.xml.crypto.Data;
import java.text.SimpleDateFormat;
import java.util.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LB server
 *
 */
public class Server {

    /**
     * Vert.x configuration
     */
    private static Vertx vertx;
    private static HttpClient httpClient;
    private static HttpServer httpServer;

    /**
     * data structure used for LoadBalancer
     */
    private static final int PORT = 80;
    // private static List<DataCenterInstance> instances;
    private static CopyOnWriteArrayList<DataCenterInstance> instances;
    private static ServerSocket serverSocket;
    // private static String lock = "lock";
    private static ReentrantLock lock;
    private static int instanceID = 0;
    private static TimeWrapper wrapper;

    /**
     * Main function
     *
     * @param args
     * @throws NamingException
     */
    public static void main(String[] args) throws NamingException {
        // create dataCenterList
        // instances = new ArrayList<DataCenterInstance>();
        instances = new CopyOnWriteArrayList<>();
        lock = new ReentrantLock();
        // init cooldown period
        wrapper = new TimeWrapper();
        wrapper.period = 0;
        // initial server socket
        initServerSocket();

        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1024));

        // Create http server
        HttpServerOptions serverOptions = new HttpServerOptions();
        httpServer = vertx.createHttpServer(serverOptions);

        // Create Router
        Router router = Router.router(vertx);
        router.route("/").handler(routingContext -> {
            routingContext.response().end("OK");
        });

        router.route("/add").handler(Server::handleAdd);
        router.route("/remove").handler(Server::handleRemove);
        router.route("/check").handler(Server::handleCheck);
        router.route("/cooldown").handler(Server::handleCooldown);
        router.route("/testh1").handler(Server::handleTest);
        router.route("/testh2").handler(Server::handleTest2);

        // Listen for the request on port 8080
        httpServer.requestHandler(router::accept).listen(8080);

        // open a new thread to run the dispatcher to handle traffic from LG
        Thread launchLoadBalancer = new Thread() {
            public void run() {
                LoadBalancer loadBalancer = new LoadBalancer(Server.serverSocket, Server.instances,  Server.wrapper, Server.lock);
                try {
                    loadBalancer.start();
                } catch (IOException e) {

                }
            }
        };
        launchLoadBalancer.start();
        
    }

    private static void handleTest(RoutingContext routingContext) {
        routingContext.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .end("dc test successfully\n");
    }

    private static void handleTest2(RoutingContext routingContext) {
        routingContext.response()
                .setStatusCode(404)
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .end("dc test successfully\n");
    }

    private static void handleCooldown(RoutingContext routingContext) {
        int cooldown = Integer.parseInt(routingContext.request().getParam("cooldown"));
        System.out.println("receive cool down  " + cooldown);
        Server.wrapper.period = cooldown;
        routingContext.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .end("set up cooldown\n");
    }

    private static void handleRemove(RoutingContext routingContext) {
        String dnsName = routingContext.request().getParam("ip");
        if (!InetAddresses.isInetAddress(dnsName)) {
            routingContext.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "text/plain; charset=utf-8")
                    .end("wrong request ip\n");
            return;
        }

            System.out.println("http remove data center instance with ip " + dnsName);
//            Iterator<DataCenterInstance> iterator = instances.iterator();
//
//            while (iterator.hasNext()) {
//                DataCenterInstance instance = iterator.next();
//                if (instance.getIP().equals(dnsName)) {
//                    iterator.remove();
//                    break;
//                }
//            }
        lock.lock();
            for (int i = 0; i < instances.size(); i++) {
                DataCenterInstance instance = instances.get(i);
                if (instance.getIP().equals(dnsName)) {
                   instances.remove(i);
                    break;
                }
            }

        lock.unlock();
        routingContext.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .end("dc instance remove correctly\n");
    }

    private static void handleCheck(RoutingContext routingContext) {
       String ret = "";
        Iterator<DataCenterInstance> iterator = instances.iterator();
        lock.lock();
        while (iterator.hasNext()) {
            DataCenterInstance instance = iterator.next();
            if (ret.length() != 0) {
                ret += ",";
            }
            ret += instance.getUrl();
        }
        lock.unlock();
        ret += "\n";
        routingContext.response().setStatusCode(200)
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .end(ret);
    }

    private static void handleAdd(RoutingContext routingContext) {
        // to get argument from http request
        String dnsName = routingContext.request().getParam("ip");
        if (!InetAddresses.isInetAddress(dnsName)) {
            routingContext.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "text/plain; charset=utf-8")
                    .end("wrong ip\n");
            return;
        }
        lock.lock();
        System.out.println("add data center instance with ip " + dnsName);
        DataCenterInstance instance = new DataCenterInstance("DC-" + instanceID, dnsName);
        instances.add(instance);
        instanceID += 1;
        lock.unlock();
        routingContext.response().setStatusCode(200)
                                 .putHeader("Content-Type", "text/plain; charset=utf-8")
                                 .end("add dc instance successfully\n");
    }

    /**
     * Initialize the socket on which the Load Balancer will receive requests from the Load Generator
     */
    private static void initServerSocket() {
        try {
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            System.err.println("ERROR: Could not listen on port: " + PORT);
            e.printStackTrace();
            System.exit(-1);
        }
    }

}

