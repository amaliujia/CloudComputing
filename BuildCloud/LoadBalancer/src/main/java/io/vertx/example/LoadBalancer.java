package io.vertx.example;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.*;

public class LoadBalancer {
	private final String USER_AGENT = "Mozilla/5.0";
	private static final int THREAD_POOL_SIZE = 4;
	private final ServerSocket socket;
	private final List<DataCenterInstance> instances;
	private final String lock;
	private final TimeWrapper timeWrapper;

	private Integer indicator;

	public LoadBalancer(ServerSocket socket, List<DataCenterInstance> instances, TimeWrapper wrapper) {
		this.socket = socket;
		this.instances = instances;
		this.timeWrapper = wrapper;
		lock = null;
	}

	public LoadBalancer(ServerSocket socket, List<DataCenterInstance> instances, String lock, TimeWrapper wrapper) {
		this.socket = socket;
		this.instances = instances;
		this.lock = lock;
		this.timeWrapper = wrapper;
	}

	// Complete this function
	public void start() throws IOException {
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		this.indicator = 0;

		Thread healthyChecker = new Thread() {
			public void run() {
				try {
					while (true) {
						if (timeWrapper.period == 0) {
							Thread.sleep(3000);
//							System.out.println("haven't set cooldown yet, sleep 3 seconds");
//							System.out.println("DC num is " + instances.size());
						} else {
							Thread.sleep(timeWrapper.period * 1000);
//							System.out.println("It is cool down, sleep " + timeWrapper.period);
//							System.out.println("DC num is " + instances.size());
							synchronized (lock) {
								Iterator<DataCenterInstance> iterator = instances.iterator();
								while (iterator.hasNext()) {
									DataCenterInstance instance = iterator.next();
									System.out.println("\nHealth check" + instance.getName());

									try {
										URL obj = new URL(instance.getUrl());
										HttpURLConnection con = (HttpURLConnection) obj.openConnection();
										con.setRequestMethod("GET");
										con.setRequestProperty("User-Agent", USER_AGENT);
										con.setConnectTimeout(5000);
										// con.setReadTimeout(1000);

										int responseCode = con.getResponseCode();
										System.out.println("\nSending 'GET' request to URL : " + instance.getUrl());
										System.out.println("Response Code : " + responseCode);
										if (responseCode != 200) {
											System.out.println(instance.getUrl() + " died, remove it!");
											iterator.remove();
										}
									} catch (Exception e) {
										System.out.println(instance.getUrl() + " died, remove it!");
										iterator.remove();
									}
								}
							}
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		healthyChecker.start();

		while (true) {
			//  TODO: send the request in round robin manner as well as handle health check

			if (instances.size() == 0) {
				continue;
			}

			indicator = (indicator % instances.size());
			Runnable requestHandler = new RequestHandler(socket.accept(), instances.get(indicator));
			executorService.execute(requestHandler);
			indicator++;
		}
	}
}
