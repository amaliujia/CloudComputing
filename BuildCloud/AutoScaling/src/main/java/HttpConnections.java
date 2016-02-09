import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by amaliujia on 16-2-8.
 */
public class HttpConnections {
    private final static String USER_AGENT = "Mozilla/5.0";

    public static void addInstanceToLoadBalancer(String lb, String ip) {
        URL obj = null;
        try {
            String url = "http://" + lb + ":8080/add?ip=" + ip;
            obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", HttpConnections.USER_AGENT);
            con.setConnectTimeout(5000);

            int responseCode = con.getResponseCode();
            System.out.println();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
