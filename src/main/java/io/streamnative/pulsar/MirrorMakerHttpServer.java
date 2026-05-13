/**
 *
 */
package io.streamnative.pulsar;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorMakerHttpServer {
    private static final Logger log = LoggerFactory.getLogger(MirrorMakerHttpServer.class);
    public int listenPort;
    public HTTPServer httpServer;
    public AtomicBoolean isClosed;

    static  class ControlHandler implements HttpHandler {
        MirrorMakerHttpServer server;
        public ControlHandler(MirrorMakerHttpServer server) {
            this.server = server;
        }

        public void handle(HttpExchange exchange) throws IOException {
            log.info("pulsar mirror worker stopped");
            this.server.isClosed.set(true);
            String response = "{\"code\":0, \"msg\":\"ok\", \"time\":" + System.currentTimeMillis() + "}";
            exchange.sendResponseHeaders(200, 0);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.flush();
            os.close();
            this.server.stop();
        }
    }

    public MirrorMakerHttpServer(int listenPort, AtomicBoolean isClosed) {
        this.listenPort = listenPort;
        this.isClosed = isClosed;
    }

    public void start() throws IOException {
        HttpServer server =  HttpServer.create(new InetSocketAddress(listenPort), 0);
        server.createContext("/stop", new ControlHandler(this));
        httpServer = new HTTPServer(server, CollectorRegistry.defaultRegistry, true);
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop();
            httpServer = null;
        }
    }
}
