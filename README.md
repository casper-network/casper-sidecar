# Event (SSE) Sidecar

### Summary of Purpose

The sidecar application will run on the same host as the node process.
It will consume the node's event stream with the aim of providing:
- A stable external interface for clients who would have previously connected directly to the node process.
- Access to historic data via convenient interfaces.
- The ability to query for filtered data.
