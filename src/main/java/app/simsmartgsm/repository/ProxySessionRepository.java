package app.simsmartgsm.repository;

import app.simsmartgsm.entity.ProxySession;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ProxySessionRepository extends JpaRepository<ProxySession, Long> {

    Optional<ProxySession> findByComPort(String comPort);

    List<ProxySession> findByStatus(String status);

    Optional<ProxySession> findByProxyPort(Integer proxyPort);

    boolean existsByProxyPort(Integer proxyPort);
}
