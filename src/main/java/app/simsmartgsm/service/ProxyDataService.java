package app.simsmartgsm.service;

import app.simsmartgsm.dto.request.ProxyStartRequest;
import app.simsmartgsm.dto.response.ProxyStatusResponse;
import app.simsmartgsm.entity.ProxySession;
import app.simsmartgsm.entity.Sim;
import app.simsmartgsm.repository.ProxySessionRepository;
import app.simsmartgsm.repository.SimRepository;
// AtCommandHelper used indirectly via PortWorker
import app.simsmartgsm.config.ComManager;
import app.simsmartgsm.uitils.DeviceIdProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ✅ ProxyDataService
 * Quản lý kết nối data qua modem EC25 và proxy server trên Windows.
 *
 * Flow:
 * 1. Gửi AT commands để modem kết nối data (NDIS mode)
 * 2. Windows tự nhận Network Adapter → có IP
 * 3. Tạo HTTP Proxy Server bind vào IP đó
 * 4. Client kết nối proxy → traffic đi qua SIM
 *
 * Mỗi SIM = 1 proxy riêng biệt (IP khác nhau)
 */
@Service
@ConditionalOnProperty(prefix = "proxy", name = "enabled", havingValue = "true")
@Slf4j
public class ProxyDataService {

    private final ProxySessionRepository proxyRepo;
    private final SimRepository simRepository;
    private final SimpMessagingTemplate messagingTemplate;
    private final ComManager comManager;

    /** Map: comPort → HttpProxyServer instance */
    private final ConcurrentHashMap<String, HttpProxyServer> proxyServers = new ConcurrentHashMap<>();

    /** Map: comPort → Process (cho rasphone/rasdial trên Windows) */
    private final ConcurrentHashMap<String, Process> dialProcesses = new ConcurrentHashMap<>();

    /** Base port cho proxy (mỗi SIM tăng dần) */
    private static final int BASE_PROXY_PORT = 10001;

    /** Maximum proxy instances */
    private static final int MAX_PROXIES = 64;

    public ProxyDataService(ProxySessionRepository proxyRepo,
                            SimRepository simRepository,
                            SimpMessagingTemplate messagingTemplate,
                            ComManager comManager) {
        this.proxyRepo = proxyRepo;
        this.simRepository = simRepository;
        this.messagingTemplate = messagingTemplate;
        this.comManager = comManager;
    }

    // =====================================================================
    // START PROXY
    // =====================================================================

    /**
     * ✅ Start proxy cho 1 SIM/COM port
     * Step 1: Kết nối data qua AT commands (NDIS mode)
     * Step 2: Detect network adapter + IP
     * Step 3: Start HTTP proxy server bind vào adapter đó
     */
    public ProxyStatusResponse startProxy(ProxyStartRequest request) {
        String comPort = request.getComPort();

        // Validate
        if (comPort == null || comPort.isBlank()) {
            throw new RuntimeException("COM port is required");
        }

        // Check if already running
        if (proxyServers.containsKey(comPort) && proxyServers.get(comPort).isRunning()) {
            log.info("⚠️ Proxy already running for {}", comPort);
            return getProxyStatus(comPort);
        }

        log.info("🚀 Starting proxy for {} with APN: {}", comPort, request.getApn());

        // Create/update session
        ProxySession session = proxyRepo.findByComPort(comPort)
                .orElse(ProxySession.builder().comPort(comPort).build());

        session.setStatus("CONNECTING");
        session.setApn(request.getApn());
        session.setStartedAt(Instant.now());
        session.setErrorMessage(null);

        // Set auth if provided
        if (request.getUsername() != null && !request.getUsername().isBlank()) {
            session.setUsername(request.getUsername());
            session.setPassword(request.getPassword());
        }

        // Get SIM info
        try {
            Optional<Sim> simOpt = simRepository.findFirstByComName(comPort);
            if (simOpt.isPresent()) {
                Sim sim = simOpt.get();
                session.setPhoneNumber(sim.getPhoneNumber());
                session.setIccid(sim.getCcid());
                session.setCarrier(sim.getSimProvider());
            }
        } catch (Exception e) {
            log.warn("⚠️ Could not get SIM info for {}: {}", comPort, e.getMessage());
        }

        session = proxyRepo.save(session);
        notifyProxyUpdate();

        try {
            // Step 1: Kết nối data qua AT commands (dùng PortWorker)
            connectData(comPort, request.getApn());

            // Step 2: Đợi Windows nhận Network Adapter + IP
            Thread.sleep(3000); // Đợi Windows detect adapter
            NetworkInfo netInfo = detectNetworkAdapter(comPort);

            if (netInfo == null || netInfo.localIp == null) {
                throw new RuntimeException("Không detect được Network Adapter. Kiểm tra driver modem.");
            }

            session.setLocalIp(netInfo.localIp);
            session.setNetworkAdapter(netInfo.adapterName);
            log.info("✅ Network adapter detected: {} (IP: {})", netInfo.adapterName, netInfo.localIp);

            // Step 3: Detect public IP
            String publicIp = detectPublicIp(InetAddress.getByName(netInfo.localIp));
            session.setPublicIp(publicIp);
            log.info("🌐 Public IP: {}", publicIp);

            // Step 4: Assign proxy port
            int proxyPort = request.getProxyPort() > 0
                    ? request.getProxyPort()
                    : assignProxyPort(comPort);

            session.setProxyPort(proxyPort);

            // Step 5: Start HTTP Proxy Server
            InetAddress bindAddr = InetAddress.getByName(netInfo.localIp);
            HttpProxyServer proxyServer = new HttpProxyServer(
                    proxyPort, bindAddr,
                    session.getUsername(), session.getPassword());
            proxyServer.start();

            proxyServers.put(comPort, proxyServer);

            // Update session
            session.setStatus("CONNECTED");
            session = proxyRepo.save(session);

            log.info("🎉 Proxy STARTED: {} → port {} → IP {}",
                    comPort, proxyPort, publicIp);

            notifyProxyUpdate();
            return toStatusResponse(session);

        } catch (Exception e) {
            log.error("❌ Failed to start proxy for {}: {}", comPort, e.getMessage(), e);
            session.setStatus("ERROR");
            session.setErrorMessage(e.getMessage());
            proxyRepo.save(session);
            notifyProxyUpdate();
            throw new RuntimeException("Không thể start proxy: " + e.getMessage());
        }
    }

    // =====================================================================
    // STOP PROXY
    // =====================================================================

    /**
     * Stop proxy cho 1 COM port
     */
    public void stopProxy(String comPort) {
        log.info("🛑 Stopping proxy for {}", comPort);

        // Stop proxy server
        HttpProxyServer server = proxyServers.remove(comPort);
        if (server != null) {
            server.stop();
        }

        // Disconnect data
        try {
            disconnectData(comPort);
        } catch (Exception e) {
            log.warn("⚠️ Error disconnecting data for {}: {}", comPort, e.getMessage());
        }

        // Update session
        proxyRepo.findByComPort(comPort).ifPresent(session -> {
            session.setStatus("STOPPED");
            proxyRepo.save(session);
        });

        log.info("✅ Proxy stopped for {}", comPort);
        notifyProxyUpdate();
    }

    public void stopAllProxies() {
        log.info("🛑 Stopping all proxies (in parallel)...");
        List<String> ports = new ArrayList<>(proxyServers.keySet());
        ports.parallelStream().forEach(comPort -> {
            try {
                stopProxy(comPort);
            } catch (Exception e) {
                log.error("❌ Error stopping proxy {}: {}", comPort, e.getMessage());
            }
        });
    }

    @PreDestroy
    public void onShutdown() {
        stopAllProxies();
    }

    // =====================================================================
    // ROTATE IP
    // =====================================================================

    /**
     * ✅ Rotate IP cho 1 proxy (ngắt kết nối → kết nối lại = IP mới)
     */
    public ProxyStatusResponse rotateIp(String comPort) {
        log.info("🔄 Rotating IP for {}", comPort);

        ProxySession session = proxyRepo.findByComPort(comPort)
                .orElseThrow(() -> new RuntimeException("Proxy session not found for " + comPort));

        if (!"CONNECTED".equals(session.getStatus())) {
            throw new RuntimeException("Proxy is not connected");
        }

        try {
            // 1. Ngắt data
            disconnectData(comPort);
            Thread.sleep(2000);

            // 2. Kết nối lại
            connectData(comPort, session.getApn());
            Thread.sleep(3000);

            // 3. Detect IP mới
            NetworkInfo netInfo = detectNetworkAdapter(comPort);
            if (netInfo != null) {
                session.setLocalIp(netInfo.localIp);
                session.setNetworkAdapter(netInfo.adapterName);

                String publicIp = detectPublicIp(InetAddress.getByName(netInfo.localIp));
                session.setPublicIp(publicIp);

                log.info("✅ IP rotated: {} → new IP: {}", comPort, publicIp);
            }

            session.setRotateCount(session.getRotateCount() + 1);
            session.setLastRotatedAt(Instant.now());
            proxyRepo.save(session);

            notifyProxyUpdate();
            return toStatusResponse(session);

        } catch (Exception e) {
            log.error("❌ IP rotation failed for {}: {}", comPort, e.getMessage());
            session.setErrorMessage("Rotate failed: " + e.getMessage());
            proxyRepo.save(session);
            throw new RuntimeException("IP rotation failed: " + e.getMessage());
        }
    }

    // =====================================================================
    // START ALL (batch)
    // =====================================================================

    /**
     * Start proxy cho tất cả SIM đang online
     */
    public List<ProxyStatusResponse> startAllProxies(String apn) {
        List<ProxyStatusResponse> results = new ArrayList<>();

        try {
            String deviceName = DeviceIdProvider.getDeviceId();
            List<Sim> sims = simRepository.findByDeviceNameAndStatus(deviceName, "ACTIVE");

            for (Sim sim : sims) {
                if (sim.getComName() == null || sim.getComName().isBlank()) continue;
                if (proxyServers.containsKey(sim.getComName())) continue; // Already running

                try {
                    ProxyStartRequest req = ProxyStartRequest.builder()
                            .comPort(sim.getComName())
                            .apn(apn != null ? apn : "internet")
                            .build();
                    results.add(startProxy(req));
                } catch (Exception e) {
                    log.error("❌ Failed to start proxy for {}: {}", sim.getComName(), e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("❌ Error in startAllProxies: {}", e.getMessage());
        }

        return results;
    }

    // =====================================================================
    // STATUS / LIST
    // =====================================================================

    /**
     * Lấy status của 1 proxy
     */
    public ProxyStatusResponse getProxyStatus(String comPort) {
        ProxySession session = proxyRepo.findByComPort(comPort).orElse(null);
        if (session == null) {
            return ProxyStatusResponse.builder()
                    .comPort(comPort)
                    .status("NOT_CONFIGURED")
                    .build();
        }

        // Sync stats from proxy server
        HttpProxyServer server = proxyServers.get(comPort);
        if (server != null) {
            session.setTotalRequests(server.getTotalRequests());
            session.setTotalBytes(server.getTotalBytes());
        }

        return toStatusResponse(session);
    }

    /**
     * Lấy danh sách tất cả proxy
     */
    public List<ProxyStatusResponse> getAllProxies() {
        List<ProxySession> sessions = proxyRepo.findAll();
        List<ProxyStatusResponse> results = new ArrayList<>();

        for (ProxySession session : sessions) {
            // Sync stats
            HttpProxyServer server = proxyServers.get(session.getComPort());
            if (server != null) {
                session.setTotalRequests(server.getTotalRequests());
                session.setTotalBytes(server.getTotalBytes());
                // Verify still running
                if (!server.isRunning() && "CONNECTED".equals(session.getStatus())) {
                    session.setStatus("ERROR");
                    session.setErrorMessage("Proxy server stopped unexpectedly");
                    proxyRepo.save(session);
                }
            }
            results.add(toStatusResponse(session));
        }

        // Also include SIMs that don't have proxy sessions yet
        try {
            String deviceName = DeviceIdProvider.getDeviceId();
            List<Sim> activeSims = simRepository.findByDeviceNameAndStatus(deviceName, "ACTIVE");
            Set<String> existingPorts = new HashSet<>();
            results.forEach(r -> existingPorts.add(r.getComPort()));

            for (Sim sim : activeSims) {
                if (sim.getComName() != null && !existingPorts.contains(sim.getComName())) {
                    results.add(ProxyStatusResponse.builder()
                            .comPort(sim.getComName())
                            .phoneNumber(sim.getPhoneNumber())
                            .carrier(sim.getSimProvider())
                            .status("AVAILABLE")
                            .build());
                }
            }
        } catch (Exception e) {
            log.debug("Could not get active SIMs: {}", e.getMessage());
        }

        return results;
    }

    // =====================================================================
    // AT COMMANDS - Data connection
    // =====================================================================

    /**
     * Kết nối data qua AT commands (NDIS mode cho Windows)
     * ✅ Sử dụng PortWorker.executeAtCommandSync() để gửi AT commands
     * qua serial port đang được PortWorker giữ (không mở port mới!)
     */
    private void connectData(String comPort, String apn) {
        log.info("📡 Connecting data via {} with APN: {}", comPort, apn);

        var worker = comManager.getWorker(comPort);
        
        // Nếu chưa có worker (có thể do SIM đang INACTIVE nên không auto-start), thử khởi tạo
        if (worker == null) {
            log.info("📡 Worker chưa chạy cho {}, thử khởi tạo...", comPort);
            Optional<Sim> simOpt = simRepository.findFirstByComName(comPort);
            if (simOpt.isPresent()) {
                worker = comManager.getOrCreateWorker(simOpt.get());
            }
        }

        if (worker == null) {
            throw new RuntimeException("Không tìm thấy thông tin SIM cho " + comPort + ". Hãy scan SIM trước.");
        }

        // Đợi worker mở port (tối đa 5s)
        int waitTime = 0;
        while (!worker.isOpen() && waitTime < 5000) {
            try {
                Thread.sleep(500);
                waitTime += 500;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (!worker.isOpen()) {
            throw new RuntimeException("PortWorker không mở được port cho " + comPort +
                    ". Vui lòng kiểm tra thiết bị hoặc scan lại SIM.");
        }

        // 1. Kiểm tra SIM ready
        String cpinResp = worker.executeAtCommandSync("AT+CPIN?", 2000);
        if (cpinResp == null || !cpinResp.contains("READY")) {
            throw new RuntimeException("SIM không sẵn sàng: " + cpinResp);
        }

        // 2. Kiểm tra đăng ký mạng
        String cregResp = worker.executeAtCommandSync("AT+CREG?", 2000);
        log.info("📶 Network registration: {}", cregResp != null ? cregResp.replace("\n", " ") : "null");

        // 3. Kiểm tra NDIS mode
        String usbnetResp = worker.executeAtCommandSync("AT+QCFG=\"usbnet\"", 2000);
        log.info("📡 USB net mode: {}", usbnetResp != null ? usbnetResp.replace("\n", " ") : "null");

        // If not in NDIS mode, switch
        if (usbnetResp != null && !usbnetResp.contains("0")) {
            log.info("🔄 Switching to NDIS mode...");
            worker.executeAtCommandSync("AT+QCFG=\"usbnet\",0", 2000);
        }

        // 4. Cấu hình APN
        String cgdcontResp = worker.executeAtCommandSync(
                "AT+CGDCONT=1,\"IP\",\"" + apn + "\"", 3000);
        log.info("📡 APN configured: {}", cgdcontResp != null ? cgdcontResp.replace("\n", " ") : "null");

        // 5. Kết nối data (NDIS)
        String connectResp = worker.executeAtCommandSync("AT+QNETDEVCTL=1,1,1", 5000);
        log.info("📡 Data connect: {}", connectResp != null ? connectResp.replace("\n", " ") : "null");

        // Fallback: CGACT
        if (connectResp == null || connectResp.contains("ERROR")) {
            log.info("📡 Fallback: Using AT+CGACT...");
            connectResp = worker.executeAtCommandSync("AT+CGACT=1,1", 5000);
            log.info("📡 CGACT result: {}", connectResp != null ? connectResp.replace("\n", " ") : "null");
        }

        // 6. Kiểm tra IP
        try { Thread.sleep(2000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        String cgpaddrResp = worker.executeAtCommandSync("AT+CGPADDR=1", 3000);
        log.info("📡 IP address: {}", cgpaddrResp != null ? cgpaddrResp.replace("\n", " ") : "null");
    }

    /**
     * Ngắt kết nối data
     * ✅ Dùng PortWorker.executeAtCommandSync() thay vì mở port mới
     */
    private void disconnectData(String comPort) {
        log.info("📡 Disconnecting data on {}", comPort);

        try {
            var worker = comManager.getWorker(comPort);
            if (worker != null && worker.isOpen()) {
                worker.executeAtCommandSync("AT+QNETDEVCTL=0,1", 3000);
                worker.executeAtCommandSync("AT+CGACT=0,1", 3000);
            } else {
                log.warn("⚠️ PortWorker not available for {}, cannot send disconnect commands", comPort);
            }
        } catch (Exception e) {
            log.warn("⚠️ Could not disconnect data on {}: {}", comPort, e.getMessage());
        }

        // Kill any dial process
        Process p = dialProcesses.remove(comPort);
        if (p != null) {
            p.destroyForcibly();
        }
    }

    // =====================================================================
    // Windows Network Detection
    // =====================================================================

    /**
     * Detect network adapter mới từ modem EC25 trên Windows.
     * Sử dụng PowerShell để query network adapters.
     */
    private NetworkInfo detectNetworkAdapter(String comPort) {
        String os = System.getProperty("os.name", "").toLowerCase();

        if (os.contains("win")) {
            return detectWindowsAdapter(comPort);
        } else {
            return detectLinuxAdapter(comPort);
        }
    }

    /**
     * Detect trên Windows dùng PowerShell
     */
    private NetworkInfo detectWindowsAdapter(String comPort) {
        try {
            // Query all mobile broadband / NDIS adapters
            ProcessBuilder pb = new ProcessBuilder(
                    "powershell", "-Command",
                    "Get-NetAdapter | Where-Object { " +
                            "$_.InterfaceDescription -match 'Quectel|NDIS|Mobile|Broadband|RNDIS|USB.*Ethernet|Remote' " +
                            "-and $_.Status -eq 'Up' " +
                            "} | Select-Object Name, InterfaceDescription, InterfaceIndex | Format-List"
            );
            pb.redirectErrorStream(true);
            Process process = pb.start();

            String output = new String(process.getInputStream().readAllBytes());
            process.waitFor(10, TimeUnit.SECONDS);

            log.debug("📡 PowerShell adapter output: {}", output);

            // Parse adapter name
            Matcher nameMatcher = Pattern.compile("Name\\s*:\\s*(.+)").matcher(output);
            Matcher idxMatcher = Pattern.compile("InterfaceIndex\\s*:\\s*(\\d+)").matcher(output);

            if (nameMatcher.find() && idxMatcher.find()) {
                String adapterName = nameMatcher.group(1).trim();
                String ifIndex = idxMatcher.group(1).trim();

                // Get IP of this adapter
                ProcessBuilder ipPb = new ProcessBuilder(
                        "powershell", "-Command",
                        "Get-NetIPAddress -InterfaceIndex " + ifIndex +
                                " -AddressFamily IPv4 | Select-Object -ExpandProperty IPAddress"
                );
                ipPb.redirectErrorStream(true);
                Process ipProcess = ipPb.start();
                String ipOutput = new String(ipProcess.getInputStream().readAllBytes());
                ipProcess.waitFor(5, TimeUnit.SECONDS);

                String ip = ipOutput.trim().split("\\r?\\n")[0].trim();
                if (!ip.isEmpty() && ip.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
                    NetworkInfo info = new NetworkInfo();
                    info.adapterName = adapterName;
                    info.localIp = ip;
                    return info;
                }
            }

            // Fallback: scan all non-default adapters
            return detectWindowsAdapterFallback();

        } catch (Exception e) {
            log.error("❌ Windows adapter detection failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Fallback: Tìm adapter có IP dạng 10.x.x.x (thường là NDIS modem)
     */
    private NetworkInfo detectWindowsAdapterFallback() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (!iface.isUp() || iface.isLoopback() || iface.isVirtual()) continue;

                String displayName = iface.getDisplayName().toLowerCase();
                // Tìm adapter liên quan tới modem
                if (displayName.contains("quectel") || displayName.contains("ndis") ||
                        displayName.contains("mobile") || displayName.contains("broadband") ||
                        displayName.contains("rndis") || displayName.contains("remote")) {

                    Enumeration<InetAddress> addrs = iface.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        InetAddress addr = addrs.nextElement();
                        if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                            NetworkInfo info = new NetworkInfo();
                            info.adapterName = iface.getDisplayName();
                            info.localIp = addr.getHostAddress();
                            return info;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("❌ Adapter fallback detection failed: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Detect adapter trên Linux (wwan, usb, ppp)
     */
    private NetworkInfo detectLinuxAdapter(String comPort) {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (!iface.isUp() || iface.isLoopback()) continue;

                String name = iface.getName().toLowerCase();
                if (name.startsWith("wwan") || name.startsWith("usb") || name.startsWith("ppp")) {
                    Enumeration<InetAddress> addrs = iface.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        InetAddress addr = addrs.nextElement();
                        if (addr instanceof Inet4Address) {
                            NetworkInfo info = new NetworkInfo();
                            info.adapterName = iface.getName();
                            info.localIp = addr.getHostAddress();
                            return info;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("❌ Linux adapter detection failed: {}", e.getMessage());
        }
        return null;
    }

    // =====================================================================
    // DETECT PUBLIC IP
    // =====================================================================

    /**
     * Detect public IP bằng cách kết nối api qua network adapter cụ thể
     */
    private String detectPublicIp(InetAddress bindAddr) {
        String[] ipServices = {
                "https://api.ipify.org",
                "https://ifconfig.me/ip",
                "https://ipinfo.io/ip"
        };

        for (String service : ipServices) {
            try {
                // Tạo connection bind qua adapter cụ thể
                URL url = URI.create(service).toURL();
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                // Note: Java HttpURLConnection doesn't support binding to specific interface directly
                // The OS routing table will route based on the adapter's IP
                // For accurate binding, we'd need a custom SocketFactory

                String ip = new String(conn.getInputStream().readAllBytes()).trim();
                conn.disconnect();

                if (ip.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
                    return ip;
                }
            } catch (Exception e) {
                log.debug("⚠️ IP detect via {} failed: {}", service, e.getMessage());
            }
        }

        return "UNKNOWN";
    }

    // =====================================================================
    // PORT RESOLUTION
    // =====================================================================

    /**
     * EC25 có nhiều cổng USB:
     * - AT port (commands): thường là COM port đang dùng cho SMS/Call
     * - Data port (NDIS): tự quản lý bởi Windows driver
     *
     * Trên Windows NDIS mode, chỉ cần gửi AT command qua AT port,
     * data tự đi qua NDIS adapter.
     */
    private String resolveDataPort(String comPort) {
        // Trên NDIS mode, AT commands gửi qua cùng AT port
        // Data tự đi qua Network Adapter, không cần port riêng
        return comPort;
    }

    /**
     * Assign proxy port tự động (không trùng)
     */
    private int assignProxyPort(String comPort) {
        // Tìm port đã assign trước đó
        Optional<ProxySession> existing = proxyRepo.findByComPort(comPort);
        if (existing.isPresent() && existing.get().getProxyPort() != null
                && existing.get().getProxyPort() > 0) {
            int port = existing.get().getProxyPort();
            if (isPortAvailable(port)) return port;
        }

        // Assign port mới
        for (int port = BASE_PROXY_PORT; port < BASE_PROXY_PORT + MAX_PROXIES; port++) {
            if (isPortAvailable(port) && !proxyRepo.existsByProxyPort(port)) {
                return port;
            }
        }

        throw new RuntimeException("No available ports for proxy (checked " +
                BASE_PROXY_PORT + "-" + (BASE_PROXY_PORT + MAX_PROXIES) + ")");
    }

    private boolean isPortAvailable(int port) {
        try (var ss = new java.net.ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // =====================================================================
    // HEALTH CHECK (Scheduled)
    // =====================================================================

    /**
     * Kiểm tra health proxy mỗi 30 giây
     */
    @Scheduled(fixedDelay = 30000, initialDelay = 10000)
    public void healthCheck() {
        if (proxyServers.isEmpty()) return;

        for (Map.Entry<String, HttpProxyServer> entry : proxyServers.entrySet()) {
            String comPort = entry.getKey();
            HttpProxyServer server = entry.getValue();

            if (!server.isRunning()) {
                log.warn("⚠️ Proxy for {} is not running, marking as ERROR", comPort);
                proxyRepo.findByComPort(comPort).ifPresent(session -> {
                    session.setStatus("ERROR");
                    session.setErrorMessage("Proxy server stopped");
                    proxyRepo.save(session);
                });
                proxyServers.remove(comPort);
                notifyProxyUpdate();
            } else {
                // Update statistics
                proxyRepo.findByComPort(comPort).ifPresent(session -> {
                    session.setTotalRequests(server.getTotalRequests());
                    session.setTotalBytes(server.getTotalBytes());
                    proxyRepo.save(session);
                });
            }
        }
    }

    // =====================================================================
    // HELPERS
    // =====================================================================

    private ProxyStatusResponse toStatusResponse(ProxySession session) {
        long uptimeSeconds = 0;
        if (session.getStartedAt() != null && "CONNECTED".equals(session.getStatus())) {
            uptimeSeconds = Duration.between(session.getStartedAt(), Instant.now()).getSeconds();
        }

        String proxyAddress = null;
        if (session.getProxyPort() != null && session.getProxyPort() > 0) {
            try {
                String hostname = InetAddress.getLocalHost().getHostAddress();
                proxyAddress = hostname + ":" + session.getProxyPort();
            } catch (Exception e) {
                proxyAddress = "localhost:" + session.getProxyPort();
            }
        }

        return ProxyStatusResponse.builder()
                .comPort(session.getComPort())
                .phoneNumber(session.getPhoneNumber())
                .carrier(session.getCarrier())
                .status(session.getStatus())
                .proxyAddress(proxyAddress)
                .proxyPort(session.getProxyPort())
                .publicIp(session.getPublicIp())
                .localIp(session.getLocalIp())
                .networkAdapter(session.getNetworkAdapter())
                .apn(session.getApn())
                .rotateCount(session.getRotateCount())
                .totalRequests(session.getTotalRequests())
                .totalBytes(session.getTotalBytes())
                .authRequired(session.getUsername() != null && !session.getUsername().isBlank())
                .username(session.getUsername())
                .startedAt(session.getStartedAt())
                .lastRotatedAt(session.getLastRotatedAt())
                .errorMessage(session.getErrorMessage())
                .uptimeSeconds(uptimeSeconds)
                .build();
    }

    private void notifyProxyUpdate() {
        try {
            List<ProxyStatusResponse> all = getAllProxies();
            messagingTemplate.convertAndSend("/topic/proxy", all);
        } catch (Exception e) {
            log.debug("Could not send proxy update: {}", e.getMessage());
        }
    }

    /** DTO nội bộ cho network info */
    private static class NetworkInfo {
        String adapterName;
        String localIp;
    }
}
