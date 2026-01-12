# Integration Tests for NebulaGraph Python Client

This directory contains integration tests that require an actual NebulaGraph server connection.

## Prerequisites

1. **Start NebulaGraph Server**
   
   Make sure NebulaGraph is running and accessible. Default address: `127.0.0.1:9669`

2. **Set Environment Variables** (Optional)
   
   If your NebulaGraph server uses different credentials, set these environment variables:

   ```bash
   export NEBULA_HOSTS="127.0.0.1:9669"
   export NEBULA_USER="root"
   export NEBULA_PASSWORD="nebula"
   ```

   Default values:
   - `NEBULA_HOSTS`: `127.0.0.1:9669`
   - `NEBULA_USER`: `root`
   - `NEBULA_PASSWORD`: `nebula`

## Running Integration Tests

```bash
# Run all integration tests
pdm run python tests/test_nebula_client_decode_integration.py -v
```

## Test Coverage

The integration tests cover:

### Geography Types
- Point decoding
- LineString decoding
- Polygon decoding

### Basic Types
- Integer
- String (including Unicode/Chinese)
- Boolean
- Date
- Duration (month-based and time-based)
- List
- Record (named tuple)
- Vector (embedding)
- Set
- Map

### Graph Types
- Node decoding
- Edge decoding
- Path decoding

## Test Data

The tests create a temporary graph named `decode` with:
- Node type `player` with various property types
- Node type `person`
- Edge type `friend` connecting person nodes

Test data is automatically cleaned up after test completion.

## Troubleshooting

### Connection Refused
```
Failed to set up test environment: Connection refused
```
Make sure NebulaGraph server is running:
```bash
# Check if NebulaGraph is listening
netstat -an | grep 9669
```

### Authentication Failed
```
Failed to set up test environment: Authentication failed
```
Verify your credentials are correct:
```bash
export NEBULA_USER="your_username"
export NEBULA_PASSWORD="your_password"
```

### Graph Already Exists
The tests automatically drop existing `decode` and `decode_type` graphs before creating new ones. If you encounter issues, manually clean up:
```bash
# Using Nebula Console
DROP GRAPH IF EXISTS decode;
DROP GRAPH TYPE IF EXISTS decode_type;
```

## Notes

- These tests require a running NebulaGraph server
- Tests create and drop temporary graphs automatically
- Tests are independent and can be run in any order
- All tests are skipped if the initial connection fails
