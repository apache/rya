package mvm.cloud.rdf.web.cloudbase.sail;

import com.google.common.io.ByteStreams;
import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

public class LoadDataServletRun extends TestCase {

    public static void main(String[] args) {
        try {
            /**
             * Create url object to POST to the running container
             */

            final InputStream resourceAsStream = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("n3trips.txt");
            URL url = new URL("http://10.41.1.95:8080/rdfTripleStoreInfer/loadrdf" +
                    "?format=N-Triples" +
                    "");
            URLConnection urlConnection = url.openConnection();
            urlConnection.setDoOutput(true);

            final OutputStream os = urlConnection.getOutputStream();

            System.out.println(resourceAsStream);
            ByteStreams.copy(resourceAsStream, os);
            os.flush();

            /**
             * Get the corresponding response from server, if any
             */
            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
