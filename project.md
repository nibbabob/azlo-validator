# Refactored Email Validator Architecture

## Project Structure
```
azlo-validator/
├── go.mod
├── azlo-validator-shared/  # Shared package
│   ├── types.go            # Common types and interfaces
│   ├── validator.go        # Core validation logic
│   ├── syntax.go           # Syntax validation
│   ├── dns.go              # DNS/MX checking
│   └── smtp.go             # SMTP verification
├── controller/             # Controller application
│   ├── main.go             # HTTP server + job distribution
│   ├── api.go              # HTTP handlers
│   ├── queue.go            # Job queue management
│   └── go.mod
└── worker/                 # Worker application
    ├── main.go             # Worker process
    ├── processor.go        # Job processing logic
    └── go.mod
```

## Architecture Overview

**Controller Application:**
- Exposes HTTP API for email validation requests
- Manages job queue and distributes work to workers
- Collects results and returns responses
- Handles batch validation requests

**Worker Application:**
- Consumes validation jobs from the queue
- Performs actual email validation using shared package
- Returns results to controller via queue

**Shared Package:**
- Contains all validation logic and common types
- Provides interfaces for queue operations
- Can be imported by both controller and worker

## Key Benefits
- **Scalability**: Multiple workers can process jobs in parallel
- **Separation of Concerns**: API handling separate from validation logic
- **Maintainability**: Shared code in one place
- **Flexibility**: Easy to swap queue implementations (Redis, RabbitMQ, etc.)

## Communication
- Controller ↔ Worker: Message queue (starting with Go channels, extensible to Redis/RabbitMQ)
- Client ↔ Controller: HTTP REST API
- Async processing with job IDs for tracki