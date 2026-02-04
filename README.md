# JobQueue

Language-agnostic background job processing with HTTP API.

## Quick Start

```bash
# Install
go install github.com/mizrahidaniel/jobqueue@latest

# Start server
jobqueue server --port 8080

# Enqueue job (from any language)
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "ml_inference", "payload": {"image_url": "https://..."}, "max_retries": 3}'

# Worker: fetch next job
curl "http://localhost:8080/jobs/next?type=ml_inference"

# Mark complete
curl -X POST http://localhost:8080/jobs/{job_id}/complete
```

## Features

- ✅ Simple HTTP API (language-agnostic)
- ✅ Automatic retries with exponential backoff
- ✅ Job timeouts & dead letter queue
- ✅ SQLite backend (zero config)
- ✅ Long-polling support
- ✅ Priority queues

## Use Cases

- ML inference (CLIP, SAM, etc.)
- Web scraping
- Report generation
- Email/notifications
- Scheduled tasks

## Architecture

```
Client → HTTP API → SQLite Queue → Worker → Process
                        ↓
                   Dead Letter Queue
```

## API

### Enqueue Job
```bash
POST /jobs
{
  "type": "ml_inference",
  "payload": {"image_url": "..."},
  "max_retries": 3,
  "timeout_seconds": 300,
  "priority": 1
}
```

### Fetch Next Job
```bash
GET /jobs/next?type=ml_inference&wait=30s
```

### Complete/Fail Job
```bash
POST /jobs/:id/complete
POST /jobs/:id/fail?error=timeout
```

### Query Status
```bash
GET /jobs/:id
```

## Status

🚧 MVP in progress - basic HTTP API + SQLite queue + worker polling
