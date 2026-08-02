package app.simsmartgsm.service;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ✅ HTTP Proxy Server thuần Java
 * Hỗ trợ HTTP CONNECT (HTTPS tunneling) và HTTP forwarding.
 * Bind traffic ra qua 1 network interface cụ thể (IP từ modem EC25).
 *
 * Chạy trên Windows, không cần phần mềm bên ngoài.
 */
@Slf4j
public class HttpProxyServer implements Closeable {

    private final int listenPort;
    private final InetAddress bindAddress;    // IP của network adapter modem
    private final String username;            // null = no auth
    private final String password;

    private ServerSocket serverSocket;
    private ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalBytes = new AtomicLong(0);
    private Thread acceptThread;

    /**
     * @param listenPort  Port để client kết nối proxy
     * @param bindAddress IP của network adapter modem (traffic sẽ đi qua adapter này)
     * @param username    Username cho proxy auth (null = no auth)
     * @param password    Password cho proxy auth
     */
    public HttpProxyServer(int listenPort, InetAddress bindAddress, String username, String password) {
        this.listenPort = listenPort;
        this.bindAddress = bindAddress;
        this.username = username;
        this.password = password;
    }

    /**
     * Start proxy server
     */
    public void start() throws IOException {
        if (running.get()) {
            log.warn("⚠️ Proxy server already running on port {}", listenPort);
            return;
        }

        serverSocket = new ServerSocket(listenPort, 50, InetAddress.getByName("0.0.0.0"));
        executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "Proxy-Worker-" + listenPort);
            t.setDaemon(true);
            return t;
        });

        running.set(true);

        acceptThread = new Thread(() -> {
            log.info("🌐 HTTP Proxy started on port {} → bind to {}", listenPort,
                    bindAddress != null ? bindAddress.getHostAddress() : "default");

            while (running.get()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(30_000);
                    executor.submit(() -> handleClient(clientSocket));
                } catch (SocketException e) {
                    if (running.get()) {
                        log.warn("⚠️ Proxy accept error: {}", e.getMessage());
                    }
                } catch (IOException e) {
                    if (running.get()) {
                        log.error("❌ Proxy accept error: {}", e.getMessage());
                    }
                }
            }
        }, "Proxy-Accept-" + listenPort);
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    /**
     * Stop proxy server
     */
    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            log.warn("⚠️ Error closing proxy server socket: {}", e.getMessage());
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        log.info("🛑 Proxy server stopped on port {}", listenPort);
    }

    @Override
    public void close() {
        stop();
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getTotalRequests() {
        return totalRequests.get();
    }

    public long getTotalBytes() {
        return totalBytes.get();
    }

    // =====================================================================
    // Client handling
    // =====================================================================

    private void handleClient(Socket clientSocket) {
        try {
            InputStream clientIn = clientSocket.getInputStream();
            OutputStream clientOut = clientSocket.getOutputStream();

            // Read first line (request line)
            String requestLine = readLine(clientIn);
            if (requestLine == null || requestLine.isEmpty()) {
                clientSocket.close();
                return;
            }

            // Read all headers
            StringBuilder headersBuilder = new StringBuilder();
            String line;
            String proxyAuth = null;
            String host = null;
            int contentLength = -1;

            while ((line = readLine(clientIn)) != null && !line.isEmpty()) {
                headersBuilder.append(line).append("\r\n");
                String lowerLine = line.toLowerCase();
                if (lowerLine.startsWith("proxy-authorization:")) {
                    proxyAuth = line.substring("proxy-authorization:".length()).trim();
                }
                if (lowerLine.startsWith("host:")) {
                    host = line.substring("host:".length()).trim();
                }
                if (lowerLine.startsWith("content-length:")) {
                    try {
                        contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
                    } catch (NumberFormatException ignored) {}
                }
            }

            // Check authentication
            if (username != null && !username.isEmpty()) {
                if (!checkAuth(proxyAuth)) {
                    String response = "HTTP/1.1 407 Proxy Authentication Required\r\n" +
                            "Proxy-Authenticate: Basic realm=\"SimSmart Proxy\"\r\n" +
                            "Content-Length: 0\r\n\r\n";
                    clientOut.write(response.getBytes(StandardCharsets.UTF_8));
                    clientOut.flush();
                    clientSocket.close();
                    return;
                }
            }

            totalRequests.incrementAndGet();

            // Parse request
            String[] parts = requestLine.split(" ", 3);
            if (parts.length < 2) {
                clientSocket.close();
                return;
            }

            String method = parts[0].toUpperCase();

            if ("CONNECT".equals(method)) {
                // HTTPS tunneling
                handleConnect(clientSocket, clientIn, clientOut, parts[1]);
            } else {
                // HTTP forwarding
                handleHttpForward(clientSocket, clientIn, clientOut, requestLine, headersBuilder.toString(),
                        host, contentLength);
            }

        } catch (SocketTimeoutException e) {
            log.debug("⏰ Proxy client timeout");
        } catch (IOException e) {
            log.debug("📡 Proxy client IO: {}", e.getMessage());
        } finally {
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException ignored) {}
        }
    }

    /**
     * Handle CONNECT method (HTTPS tunneling)
     * Client gửi: CONNECT host:port HTTP/1.1
     * Server trả: 200 Connection Established, rồi tunnel 2 chiều
     */
    private void handleConnect(Socket clientSocket, InputStream clientIn, OutputStream clientOut,
                               String hostPort) {
        String targetHost;
        int targetPort;

        try {
            String[] hp = hostPort.split(":");
            targetHost = hp[0];
            targetPort = hp.length > 1 ? Integer.parseInt(hp[1]) : 443;
        } catch (Exception e) {
            log.warn("⚠️ Invalid CONNECT target: {}", hostPort);
            return;
        }

        Socket targetSocket = null;
        try {
            // Connect to target through specific network interface
            targetSocket = createBoundSocket(targetHost, targetPort);
            targetSocket.setSoTimeout(30_000);

            // Send 200 OK to client
            String response = "HTTP/1.1 200 Connection Established\r\n\r\n";
            clientOut.write(response.getBytes(StandardCharsets.UTF_8));
            clientOut.flush();

            // Bi-directional tunnel
            tunnel(clientSocket, targetSocket);

        } catch (IOException e) {
            log.debug("❌ CONNECT to {}:{} failed: {}", targetHost, targetPort, e.getMessage());
            try {
                String errResponse = "HTTP/1.1 502 Bad Gateway\r\n\r\n";
                clientOut.write(errResponse.getBytes(StandardCharsets.UTF_8));
                clientOut.flush();
            } catch (IOException ignored) {}
        } finally {
            closeQuietly(targetSocket);
        }
    }

    /**
     * Handle HTTP forward (non-HTTPS)
     */
    private void handleHttpForward(Socket clientSocket, InputStream clientIn, OutputStream clientOut,
                                   String requestLine, String headers, String host, int contentLength) {
        if (host == null || host.isEmpty()) {
            log.debug("⚠️ No Host header found");
            return;
        }

        String targetHost;
        int targetPort;
        try {
            if (host.contains(":")) {
                String[] hp = host.split(":");
                targetHost = hp[0];
                targetPort = Integer.parseInt(hp[1]);
            } else {
                targetHost = host;
                targetPort = 80;
            }
        } catch (Exception e) {
            log.warn("⚠️ Invalid host: {}", host);
            return;
        }

        Socket targetSocket = null;
        try {
            targetSocket = createBoundSocket(targetHost, targetPort);
            targetSocket.setSoTimeout(30_000);

            OutputStream targetOut = targetSocket.getOutputStream();
            InputStream targetIn = targetSocket.getInputStream();

            // Rewrite request line (remove absolute URL, keep relative path)
            String rewrittenRequest = rewriteRequestLine(requestLine);

            // Remove proxy-specific headers, forward the rest
            String cleanHeaders = removeProxyHeaders(headers);

            // Send request to target
            targetOut.write((rewrittenRequest + "\r\n").getBytes(StandardCharsets.UTF_8));
            targetOut.write((cleanHeaders + "\r\n").getBytes(StandardCharsets.UTF_8));

            // Forward request body if any
            if (contentLength > 0) {
                transferBytes(clientIn, targetOut, contentLength);
            }
            targetOut.flush();

            // Forward response back to client
            transferAll(targetIn, clientOut);

        } catch (IOException e) {
            log.debug("❌ HTTP forward to {}:{} failed: {}", targetHost, targetPort, e.getMessage());
        } finally {
            closeQuietly(targetSocket);
        }
    }

    // =====================================================================
    // Network binding
    // =====================================================================

    /**
     * Tạo Socket kết nối tới target, bind qua network adapter cụ thể
     */
    private Socket createBoundSocket(String host, int port) throws IOException {
        Socket socket = new Socket();

        // Bind to specific network adapter (modem IP)
        if (bindAddress != null) {
            socket.bind(new InetSocketAddress(bindAddress, 0));
        }

        socket.connect(new InetSocketAddress(host, port), 10_000);
        return socket;
    }

    // =====================================================================
    // Tunneling
    // =====================================================================

    private void tunnel(Socket client, Socket target) {
        CountDownLatch latch = new CountDownLatch(2);

        // Client → Target
        Thread t1 = new Thread(() -> {
            try {
                transferStream(client.getInputStream(), target.getOutputStream());
            } catch (IOException ignored) {
            } finally {
                latch.countDown();
                closeQuietly(target);  // Close target when client stops sending
            }
        }, "Tunnel-C2T");
        t1.setDaemon(true);

        // Target → Client
        Thread t2 = new Thread(() -> {
            try {
                transferStream(target.getInputStream(), client.getOutputStream());
            } catch (IOException ignored) {
            } finally {
                latch.countDown();
                closeQuietly(client);  // Close client when target stops sending
            }
        }, "Tunnel-T2C");
        t2.setDaemon(true);

        t1.start();
        t2.start();

        try {
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void transferStream(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
            out.flush();
            totalBytes.addAndGet(read);
        }
    }

    private void transferBytes(InputStream in, OutputStream out, int count) throws IOException {
        byte[] buffer = new byte[8192];
        int remaining = count;
        while (remaining > 0) {
            int read = in.read(buffer, 0, Math.min(buffer.length, remaining));
            if (read == -1) break;
            out.write(buffer, 0, read);
            remaining -= read;
            totalBytes.addAndGet(read);
        }
    }

    private void transferAll(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
            out.flush();
            totalBytes.addAndGet(read);
        }
    }

    // =====================================================================
    // Auth
    // =====================================================================

    private boolean checkAuth(String proxyAuthHeader) {
        if (proxyAuthHeader == null) return false;

        try {
            if (proxyAuthHeader.toLowerCase().startsWith("basic ")) {
                String encoded = proxyAuthHeader.substring(6).trim();
                String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
                String[] creds = decoded.split(":", 2);
                return creds.length == 2 &&
                        username.equals(creds[0]) &&
                        password.equals(creds[1]);
            }
        } catch (Exception e) {
            log.debug("⚠️ Auth decode error: {}", e.getMessage());
        }
        return false;
    }

    // =====================================================================
    // Utilities
    // =====================================================================

    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                int next = in.read();
                if (next == '\n') break;
                sb.append((char) c);
                if (next != -1) sb.append((char) next);
            } else if (c == '\n') {
                break;
            } else {
                sb.append((char) c);
            }
        }
        return c == -1 && sb.isEmpty() ? null : sb.toString();
    }

    private String rewriteRequestLine(String requestLine) {
        // "GET http://example.com/path HTTP/1.1" → "GET /path HTTP/1.1"
        String[] parts = requestLine.split(" ", 3);
        if (parts.length < 3) return requestLine;

        String url = parts[1];
        if (url.startsWith("http://") || url.startsWith("https://")) {
            try {
                URI uri = new URI(url);
                String path = uri.getRawPath();
                if (path == null || path.isEmpty()) path = "/";
                String query = uri.getRawQuery();
                if (query != null) path += "?" + query;
                return parts[0] + " " + path + " " + parts[2];
            } catch (Exception e) {
                return requestLine;
            }
        }
        return requestLine;
    }

    private String removeProxyHeaders(String headers) {
        StringBuilder cleaned = new StringBuilder();
        for (String header : headers.split("\r\n")) {
            String lower = header.toLowerCase();
            if (lower.startsWith("proxy-authorization:") ||
                    lower.startsWith("proxy-connection:")) {
                continue; // Skip proxy-specific headers
            }
            cleaned.append(header).append("\r\n");
        }
        return cleaned.toString();
    }

    private void closeQuietly(Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }
}
