package app.simsmartgsm;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Rectangle2D;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.web.WebView;
import javafx.stage.Screen;
import javafx.stage.Stage;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * JavaFX Desktop Wrapper for GSM Smart Application
 * Wraps the Spring Boot web app in a native desktop window
 */
public class GsmDesktopApp extends Application {

    private static ConfigurableApplicationContext springContext;
    private static final String APP_URL = "http://localhost:8080";

    @Override
    public void init() throws Exception {
        // Start Spring Boot application in background
        System.out.println("🚀 Starting Spring Boot backend...");
        SpringApplication app = new SpringApplication(SimsmartGsmApplication.class);
        app.setHeadless(false);
        springContext = app.run();
        System.out.println("✅ Spring Boot started successfully");
    }

    @Override
    public void start(Stage primaryStage) {
        System.out.println("🖥️ Creating desktop window...");

        // Create WebView to display web UI
        WebView webView = new WebView();
        webView.getEngine().setJavaScriptEnabled(true);

        // Load the Spring Boot web application
        webView.getEngine().load(APP_URL);

        // Calculate suitable dimensions based on screen bounds
        Rectangle2D screenBounds = Screen.getPrimary().getVisualBounds();
        // Cho ứng dụng rộng lên 90% màn hình để xem được tối đa dữ liệu
        double width = screenBounds.getWidth() * 0.90;
        double height = screenBounds.getHeight() * 0.95;

        // Create scene with dynamic size
        Scene scene = new Scene(webView, width, height);
        
        // Tự động mở thủ công full window
        primaryStage.setMaximized(true);

        // Configure stage (window)
        primaryStage.setTitle("GSM Smart Application");

        // Add Application Icon
        try {
            primaryStage.getIcons()
                    .add(new javafx.scene.image.Image(getClass().getResourceAsStream("/static/images/logo.png")));
        } catch (Exception e) {
            System.err.println("Could not load application icon: " + e.getMessage());
        }

        primaryStage.setScene(scene);
        primaryStage.setResizable(true); // Cho phép phóng to/thu nhỏ
        // primaryStage.setMaximized(true); // Đã tắt để ứng dụng không hiển thị quá to

        // Center on screen
        primaryStage.setX((screenBounds.getWidth() - width) / 2);
        primaryStage.setY((screenBounds.getHeight() - height) / 2);

        // Handle window close event (no confirmation popup needed)
        primaryStage.setOnCloseRequest(event -> {
            System.out.println("🛑 User closed the window, shutting down immediately...");
            shutdown();
        });

        // Show window
        primaryStage.show();
        System.out.println("✅ Desktop window created");
    }

    @Override
    public void stop() {
        shutdown();
    }

    /**
     * Gracefully shutdown Spring Boot and JavaFX
     */
    private void shutdown() {
        try {
            if (springContext != null && springContext.isActive()) {
                System.out.println("🧹 Closing Spring Boot context...");
                springContext.close();
            }
            Platform.exit();
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        // Launch JavaFX application
        launch(args);
    }
}
