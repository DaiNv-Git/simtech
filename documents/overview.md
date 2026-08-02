# SimSmart GSM - System Overview

## Project Description

SimSmart GSM is a Java Spring Boot application for managing multiple GSM modems. It provides APIs for sending/receiving SMS, making/receiving calls, recording calls, and managing SIM cards across multiple COM ports.

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Backend | Java 17+, Spring Boot 3.x |
| Database | MySQL / PostgreSQL (JPA/Hibernate) |
| WebSocket | Spring WebSocket (STOMP) |
| Frontend | HTML5, JavaScript, Bootstrap |
| Build Tool | Maven |
| Communication | AT Commands via Serial Port (jSerialComm) |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (Browser)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  SIM Panel   │  │  SMS Panel   │  │    Call Panel        │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
│         │                 │                      │               │
│         └─────────────────┴──────────────────────┘               │
│                           │                                       │
│                    WebSocket (STOMP)                             │
└───────────────────────────┼───────────────────────────────────────┘
                            │
┌───────────────────────────┼───────────────────────────────────────┐
│                           ▼                    Spring Boot        │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    GsmController (REST API)                  │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                │                                  │
│  ┌──────────────┐  ┌──────────┴──────────┐  ┌─────────────────┐ │
│  │  GsmService  │  │   SimSyncService    │  │   CallService   │ │
│  └──────┬───────┘  └──────────┬──────────┘  └────────┬────────┘ │
│         │                     │                       │           │
│  ┌──────┴───────┐  ┌──────────┴──────────┐  ┌────────┴────────┐ │
│  │  ComManager  │  │   SimRepository     │  │ CallRepository  │ │
│  └──────┬───────┘  └─────────────────────┘  └─────────────────┘ │
│         │                                                          │
│  ┌──────┴─────────────────────────────────────────────────────┐  │
│  │                     PortWorker (per COM port)              │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │  │
│  │  │   COM1   │  │   COM2   │  │   COM3   │  │   COM4   │  │  │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  │  │
│  └───────┼────────────┼────────────┼────────────┼────────────┘  │
└──────────┼────────────┼────────────┼────────────┼───────────────┘
           │            │            │            │
      ┌────┴────┐  ┌───┴────┐  ┌───┴────┐  ┌───┴────┐
      │ GSM 1   │  │ GSM 2   │  │ GSM 3  │  │ GSM 4  │
      │ Modem   │  │ Modem   │  │ Modem  │  │ Modem  │
      └─────────┘  └─────────┘  └─────────┘  └─────────┘
```

## Core Components

### 1. ComManager
- **Purpose**: Manages all COM port connections
- **Key Features**:
  - Creates and manages `PortWorker` instances per COM port
  - Tracks active/inactive ports
  - Provides port resolution mapping

### 2. PortWorker
- **Purpose**: Thread-safe worker for each COM port
- **Key Features**:
  - Task queue system (SMS, Call, Scan operations)
  - URC (Unsolicited Result Code) detection
  - Call state management (prevents conflicts during calls)
  - Session-based architecture for operations

### 3. SimSyncService
- **Purpose**: SIM card synchronization and management
- **Key Features**:
  - Scan all COM ports for SIM cards
  - Read SIM information (ICCID, IMSI, MSISDN)
  - Sync with database
  - WebSocket notifications

### 4. CallService
- **Purpose**: Call management
- **Key Features**:
  - Make outgoing calls
  - Answer incoming calls
  - Call recording
  - Upload recordings to cloud
  - Task tracking for cleanup

### 5. GsmService
- **Purpose**: Main service layer
- **Key Features**:
  - Orchestrates all operations
  - Manages SMS operations
  - Interfaces with repositories

## WebSocket Topics

| Topic | Purpose |
|-------|---------|
| `/topic/sims` | Full SIM list updates |
| `/topic/sms` | New SMS notifications |
| `/topic/call-status` | Call status updates |
| `/topic/sms-unread-count` | Unread SMS count |

## AT Commands Used

| Command | Purpose |
|---------|---------|
| `AT` | Test connection |
| `ATE0` | Echo off |
| `AT+CLIP=1` | Enable caller ID |
| `AT+CRC=1` | Enable call indication |
| `AT+CLCC` | List current calls |
| `AT+CMGL` | List SMS |
| `AT+CMGS` | Send SMS |
| `AT+CREC` | Call recording |
| `ATD` | Dial number |
| `ATH` | Hang up call |

## Key Design Decisions

1. **Per-Port Worker Pattern**: Each COM port has dedicated worker to prevent concurrent access issues
2. **Task Queue**: Operations are queued per port to ensure sequential execution
3. **URC Protection**: URC detection disabled during calls to prevent conflicts
4. **Session-Based**: Operations use session IDs for tracking and cleanup
5. **Thread Tracking**: All scheduled tasks tracked for proper cleanup on shutdown

## Database Schema

### Sim
- id, comPort, phoneNumber, iccid, imsi, operator, signal
- status (ACTIVE, INACTIVE, REPLACED, UNKNOWN)
- deviceName, createdAt, updatedAt

### SmsMessageEntity
- id, comPort, sender, recipient, message, status
- direction (INBOUND, OUTBOUND)
- sentAt, receivedAt, isRead

### CallRecordEntity
- id, fromNumber, toNumber, comPort, status
- callStartTime, callEndTime, recordFile
- simPhone, deviceName, orderId

### AppSettings
- id, scanInterval, maxRetry, timeout
- recordingEnabled, uploadUrl, publicUrl
