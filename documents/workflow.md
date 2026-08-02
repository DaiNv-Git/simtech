# SimSmart GSM - Workflow Documentation

## System Workflows

### 1. Startup Flow

```
┌──────────────┐
│ Application  │
│   Start      │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Load Config  │
│ (application │
│    .yml)     │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Initialize   │
│ ComManager   │
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌─────────────┐
│ Start        │────▶│ PortWorkers │
│ Scheduled    │     │   Created   │
│   Tasks      │     └─────────────┘
└──────────────┘
       │
       ▼
┌──────────────┐
│  Ready for   │
│   Requests   │
└──────────────┘
```

### 2. SIM Scan Flow

```
┌──────────────────┐
│ API: /sim/scan   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Scan all COM     │
│ ports via        │
│ PortResolver     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌───────────────┐
│ For each port:   │────▶│ Check if      │
│                  │     │ accessible    │
└────────┬─────────┘     └───────┬───────┘
         │                       │
         ▼                       ▼
    ┌─────────┐            ┌──────────┐
    │ Create  │            │  Skip    │
    │ Worker  │            │  Port    │
    └────┬────┘            └──────────┘
         │
         ▼
┌──────────────────┐
│ Send AT commands │
│ - AT+CIMI (IMSI) │
│ - AT+CICCID      │
│ - AT+CNUM        │
│ - AT+CSQ         │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Save/Update in   │
│   Database       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Send via         │
│ WebSocket:       │
│ /topic/sims      │
└──────────────────┘
```

### 3. Send SMS Flow

```
┌──────────────────┐
│ API: /sms/send   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Validate Request │
│ (phone, comPort) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Get Worker from  │
│   ComManager     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Create Task      │
│ Task.sms()       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Enqueue to       │
│ PortWorker queue │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌───────────────┐
│ Worker processes │────▶│ Send AT+CMGS  │
│   queue          │     │ command       │
└────────┬─────────┘     └───────┬───────┘
         │                       │
         ▼                       ▼
    ┌─────────┐            ┌──────────┐
    │ Update  │            │  Retry   │
    │ Status  │            │  (max 3) │
    │ SUCCESS │            └──────────┘
    └────┬────┘
         │
         ▼
┌──────────────────┐
│ WebSocket notify │
│ /topic/sms       │
└──────────────────┘
```

### 4. Make Call Flow

```
┌──────────────────┐
│ API: /call/make  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Create CallRecord│
│ Status: PENDING  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Worker Available?│
└────┬─────────┬───┘
     │ Yes     │ No
     ▼         ▼
┌────────┐ ┌─────────────┐
│Enqueue │ │ Direct Call │
│ Task   │ │ (fallback)  │
└───┬────┘ └──────┬──────┘
    │             │
    └──────┬──────┘
           ▼
┌──────────────────┐
│ ATD<number>;     │
│ Dial command     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Monitor CLCC     │
│ (call state)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Connected?       │
└────┬─────────┬───┘
     │ Yes     │ No
     ▼         ▼
┌────────┐ ┌──────────┐
│SUCCESS │ │NO_ANSWER │
└───┬────┘ └────┬─────┘
    │            │
    └─────┬──────┘
          ▼
┌──────────────────┐
│ ATH (Hang up)    │
│ + Stop Recording │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Upload Recording │
│   to Cloud       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Update DB +      │
│ WebSocket notify │
└──────────────────┘
```

### 5. Incoming Call Flow

```
┌──────────────────┐
│ URC Detected:    │
│   RING or CLIP   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Check: Call in   │
│   progress?      │
└────┬─────────┬───┘
     │ No      │ Yes
     ▼         ▼
┌────────┐ ┌────────────┐
│Process ││ Ignore URC │
│ Call   ││ (busy)     │
└───┬────┘ └────────────┘
    │
    ▼
┌──────────────────┐
│ Send WebSocket   │
│ /topic/call-in   │
└──────────────────┘
```

### 6. SMS Receive Flow (URC)

```
┌──────────────────┐
│ URC: +CMTI       │
│   "SM",<idx>     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Check: Call in   │
│   progress?      │
└────┬─────────┬───┘
     │ No      │ Yes
     ▼         ▼
┌────────┐ ┌────────────┐
│Queue   ││ Defer URC  │
│ Scan   ││ (scan later)│
└───┬────┘ └────────────┘
    │
    ▼
┌──────────────────┐
│ AT+CMGL="ALL"    │
│ Read all SMS     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Parse & Save to  │
│   Database       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ WebSocket notify │
│ /topic/sms       │
│ /topic/unread    │
└──────────────────┘
```

## State Transition Diagrams

### Call Status States

```
     ┌─────────┐
     │ PENDING │
     └────┬────┘
          │
          ▼
     ┌─────────┐
     │ QUEUED  │ ◀─── (via worker)
     └────┬────┘
          │
          ▼
     ┌─────────┐
     │ ONGOING │
     └────┬────┘
          │
    ┌─────┴─────┐
    │           │
    ▼           ▼           ▼           ▼
┌───────┐  ┌─────────┐  ┌─────┐  ┌──────────┐
│SUCCESS│  │NO_ANSWER│  │BUSY │  │  FAILED  │
└───────┘  └─────────┘  └─────┘  └──────────┘
```

### SIM Status States

```
     ┌─────────┐
     │UNKNOWN  │
     └────┬────┘
          │ Scan
          ▼
     ┌─────────┐      ┌──────────┐
     │ ACTIVE  │─────▶│REPLACED  │
     └────┬────┘      └──────────┘
          │ Disconnect
          ▼
     ┌─────────┐
     │INACTIVE │
     └─────────┘
```

### SMS Status States

```
┌─────────┐      ┌─────────┐
│ PENDING │─────▶│ QUEUED  │
└────┬────┘      └────┬────┘
     │                │
     │                ▼
     │           ┌─────────┐      ┌─────────┐
     └──────────▶│ SENDING │─────▶│  SENT   │
                  └────┬────┘      └─────────┘
                       │
                       ▼
                  ┌─────────┐
                  │  OUTBOX │
                  └─────────┘
```

## Error Handling Flows

### Timeout/Retry Pattern

```
┌─────────────┐
│ Try Action  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Success?   │
└──┬──────┬───┘
   │ Yes  │ No
   ▼      ▼
┌────┐ ┌────────────┐
│Done│ │Retry < Max?│
└────┘ └──┬──────┬───┘
         │Yes   │No
         ▼      ▼
    ┌────────┐ ┌──────────┐
    │Wait &  │ │ Mark     │
    │Retry   │ │ FAILED   │
    └───┬────┘ └──────────┘
        │
        └────────┘
```

### Port Worker Error Recovery

```
┌──────────────┐
│ Task Failed  │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Log Error    │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Check Port   │
│  Still Open? │
└──┬───────┬───┘
   │Yes    │No
   ▼       ▼
┌──────┐ ┌─────────┐
│ Next ││ Reopen  │
│ Task ││ Port    │
└──────┘ └────┬────┘
             │
             ▼
        ┌──────────┐
        │ Next Task│
        └──────────┘
```

## Configuration Flow

### Application Properties

```yaml
# COM Port Configuration
gsm:
  com-ports: COM3,COM4,COM5,COM6
  baud-rate: 115200
  scan-interval: 300000  # 5 minutes

  # Recording
  record:
    upload-dir: /var/www/html/recordings
    upload-url: https://api.example.com/recordings/upload
    public-url: https://cdn.example.com/recordings/

  # SMS
  sms:
    max-retry: 3
    timeout: 30000
    encoding: UCS2

  # Call
  call:
    default-duration: 30
    recording-enabled: true
```

## Monitoring & Maintenance

### Health Check Flow

```
┌──────────────┐
│ GET /stats   │
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│ Count Active SIM │
│ Count Queued SMS │
│ Count Active Call│
│ Check Disk Space │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Return JSON      │
│ Response         │
└──────────────────┘
```

### Cleanup Tasks

```
┌──────────────────┐
│ Daily Cleanup    │
│ (Scheduled)      │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐     ┌─────────────────┐
│ Remove Duplicate │────▶│ Keep newest     │
│ SIM Records      │     │ by CCID         │
└──────────────────┘     └─────────────────┘
       │
       ▼
┌──────────────────┐     ┌─────────────────┐
│ Remove Old       │────▶│ Older than 30   │
│ REPLACED Sims    │     │ days            │
└──────────────────┘     └─────────────────┘
