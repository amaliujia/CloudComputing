import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by amaliujia on 16-2-8.
 */
public class HttpConnections {
    private final static String USER_AGENT = "Mozilla/5.0";

    public static void addInstanceToLoadBalancer(String lb, DataCenterInstance instance) {
        URL obj = null;
        try {
            String url = "http://" + lb + ":8080/add?ip=" + instance.getIP();
            obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", HttpConnections.USER_AGENT);
            con.setConnectTimeout(args.timeout);

            int responseCode = con.getResponseCode();
            System.out.println("\nSend add equest to Load Balancer, URL : " + instance.getIP());
            System.out.println("Response Code : " + responseCode);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void removeInstanceFromLoadBalancer(String lb, DataCenterInstance instance) {
        URL obj = null;
        try {
            String url = "http://" + lb + ":8080/remove?ip=" + instance.getIP();
            obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", HttpConnections.USER_AGENT);
            con.setConnectTimeout(args.timeout);

            int responseCode = con.getResponseCode();
            System.out.println("\nSending remove request to Load Balancer, URL : " + instance.getIP());
            System.out.println("Response Code : " + responseCode);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
