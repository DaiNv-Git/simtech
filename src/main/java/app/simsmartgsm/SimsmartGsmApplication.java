package app.simsmartgsm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties
public class SimsmartGsmApplication {

	public static void main(String[] args) {
		// Launch JavaFX Desktop App instead of Spring Boot directly
		// JavaFX will start Spring Boot in background during init()
		javafx.application.Application.launch(GsmDesktopApp.class, args);
	}
}
