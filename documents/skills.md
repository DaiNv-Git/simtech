# SimSmart GSM - Development Skills & Capabilities

## Core Skills Demonstrated

### 1. Multi-threaded Programming
- **ScheduledExecutorService** for concurrent task scheduling
- **ConcurrentHashMap** for thread-safe state management
- **AtomicBoolean/AtomicReference** for lock-free synchronization
- **CompletableFuture** for async operations
- **Thread pool management** with proper cleanup (@PreDestroy)

### 2. Serial Communication
- **jSerialComm** library for COM port access
- **AT Command** handling with timeout management
- **Buffered reading** with response parsing
- **Port resolution** for different OS (Windows/Linux/Mac)

### 3. Spring Framework
- **Spring Boot** application architecture
- **WebSocket (STOMP)** for real-time notifications
- **JPA/Hibernate** for database operations
- **REST API** with proper error handling
- **Scheduled tasks** with @Scheduled annotation
- **Dependency injection** patterns

### 4. Design Patterns
| Pattern | Usage |
|---------|-------|
| Worker Thread | PortWorker per COM port |
| Producer-Consumer | Task queue in PortWorker |
| Session Object | Session-based operations |
| Repository | Data access layer |
| Service Layer | Business logic separation |
| Observer | WebSocket notifications |
| Factory | Task creation (Task.call(), Task.sms()) |

### 5. Error Handling
- **Try-catch-finally** with proper resource cleanup
- **Status tracking** (PENDING, QUEUED, ONGOING, SUCCESS, FAILED)
- **Retry mechanisms** with configurable max attempts
- **Graceful degradation** (fallback to direct call when worker unavailable)

### 6. Resource Management
- **@PreDestroy** hooks for cleanup
- **Task tracking** maps for cancelling scheduled work
- **Connection pooling** via ComManager
- **File handle cleanup** for recording uploads

## AT Command Expertise

### Basic Commands
```
AT              - Test connection
ATE0            - Disable echo
AT+CLIP=1       - Enable calling line identification
AT+CRC=1        - Enable call result code
AT+CMGF=1       - Set SMS to text mode
AT+CLCC         - List current calls
```

### SMS Operations
```
AT+CMGL="ALL"   - List all SMS
AT+CMGS="<num>" - Send SMS (followed by message Ctrl+Z)
AT+CMGD=<idx>   - Delete SMS by index
AT+CPMS?        - Query SMS storage
```

### Call Operations
```
ATD<number>;    - Dial number
ATH             - Hang up
ATA             - Answer call
AT+CREC=1       - Enable recording
AT+CREC=0       - Stop recording
```

### Network/Information
```
AT+CIMI         - Get IMSI
AT+CICCID       - Get ICCID
AT+CNUM         - Get subscriber number
AT+CSQ          - Signal quality
AT+COPS?        - Current operator
AT+CPIN?        - SIM PIN status
AT+CREG?        - Network registration
```

## URC (Unsolicited Result Code) Handling

### URC Events Monitored
```
+CMTI: "SM",<idx>    - New SMS received
+CLIP: <number>      - Incoming caller ID
+CRING: <type>       - Ring indication
+CIEV: ...           - Indicator event
```

### URC Protection Strategy
```java
// Disable URC during calls to prevent conflicts
if (currentCallType == null) {
    checkForUrcNotification();
    checkSimulatedUrc();
}
```

## WebSocket Real-time Communication

### Topics
```java
/topic/sims              - Full SIM list updates
/topic/sms               - New SMS received
/topic/call-status       - Call state changes
/topic/sms-unread-count  - Unread count updates
```

### Message Format
```json
{
  "sim": "0123456789",
  "com": "COM3",
  "status": "SUCCESS",
  "timestamp": "2025-01-26T10:30:00Z"
}
```

## Database Schema Design

### SIM Entity
```java
@Entity
public class Sim {
    Long id;              // Primary key
    String comPort;       // COM port name
    String phoneNumber;   // MSISDN (may be null)
    String iccid;         // SIM card ID
    String imsi;          // International mobile subscriber ID
    String operator;      // Network operator
    Integer signal;       // Signal strength (0-31)
    SimStatus status;     // ACTIVE, INACTIVE, REPLACED
    String deviceName;    // Modem device name
    Instant createdAt;
    Instant updatedAt;
}
```

### Call Record Entity
```java
@Entity
public class CallRecordEntity {
    Long id;
    String fromNumber;
    String toNumber;
    String comPort;
    String status;        // ONGOING, SUCCESS, BUSY, NO_ANSWER
    Instant callStartTime;
    Instant callEndTime;
    String recordFile;    // URL to recording
    String simPhone;
    String orderId;       // External reference
}
```

## Common Problems & Solutions

| Problem | Solution |
|---------|----------|
| Thread leaks on shutdown | Track all ScheduledFutures, cancel in @PreDestroy |
| Port conflicts | Use ComManager single-source-of-truth |
| Race conditions on updates | Use ConcurrentHashMap, atomic flags |
| URC interference during calls | Check currentCallType before URC processing |
| SMS detection complexity | Removed SMS-based number detection |
| Data loss in UI | Single source of truth for WebSocket updates |
| Exception not updating DB | Explicit status updates in catch blocks |
| Early call termination | CLCC polling with non-empty response check |

## Code Quality Practices

1. **Logging**: Slf4j with emojis for easy scanning
2. **Naming**: Clear, descriptive names (makeCall, detectNumbersOnly)
3. **Comments**: Section headers with visual separators
4. **Error messages**: Descriptive with context
5. **Resource cleanup**: Try-with-resources, finally blocks
6. **Immutability**: Final fields where possible
7. **Separation of concerns**: Service/Controller/Repository layers
