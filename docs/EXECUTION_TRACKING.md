# Execution Tracking and Timeout Monitoring

This document describes the execution tracking and timeout monitoring features added to improve observability and debugging of stuck Jupyter notebook executions.

## Overview

The execution tracking system provides:

1. **Execution Event Tracking**: Logs every cell execution with start/end times, duration, and success status
2. **Execution Metadata Publishing**: Publishes structured metadata to the webapp for monitoring
3. **ZMQ Message Flow Logging**: Enhanced debugging of kernel-server communication
4. **Execution Timeout Monitoring**: Optional monitoring with warnings and auto-interrupt for stuck executions

## Features

### 1. Execution Event Tracking

**Location**: `deepnote_toolkit/execution_tracking.py`

Automatically tracks all notebook cell executions and logs:
- Execution start with cell preview
- Execution end with duration and success status
- Error types if execution fails

**Log Format**:
```
EXEC_START | count=1 | cell_id=12345 | preview=import pandas as pd\ndf = pd.read_csv(...
EXEC_END | count=1 | duration=2.45s | success=True
```

This is **enabled by default** and requires no configuration.

### 2. Execution Metadata Publishing

**Location**: `deepnote_toolkit/ipython_utils.py`

Publishes execution metadata to the webapp via the `application/vnd.deepnote.execution-metadata+json` MIME type.

**Metadata Structure**:
```python
{
    "execution_count": 1,
    "duration_seconds": 2.45,
    "success": True,
    "error_type": "KeyError"  # Only present if execution failed
}
```

This allows the webapp to:
- Display execution duration in the UI
- Track execution history
- Alert on failed executions
- Build execution analytics

### 3. ZMQ Session Debug Logging

**Location**: `deepnote_core/resources/jupyter/jupyter_server_config.py:817`

Enables detailed ZMQ message logging by setting:
```python
c.Session.debug = True
```

This logs all ZMQ messages exchanged between the Jupyter server and kernel, which is critical for debugging:
- Message delays
- Message loss
- Protocol issues
- Kernel communication problems

**Enhanced Logging Configuration** (line 418-426):
```python
c.ServerApp.logging_config = {
    "loggers": {
        "tornado.access": {"level": "DEBUG"},
        "jupyter_server.serverapp": {"level": "DEBUG"},
        "jupyter_client.session": {"level": "DEBUG"},
        "execution_tracker": {"level": "INFO"},
        "execution_tracking": {"level": "INFO"},
    }
}
```

### 4. Execution Timeout Monitoring (Optional)

**Location**: `deepnote_toolkit/execution_timeout.py`

Optional feature that monitors execution duration and can:
- Send warnings when executions exceed a threshold
- Report to the webapp
- Automatically interrupt stuck executions (if enabled)

**Configuration via Environment Variables**:

```bash
# Enable timeout monitoring (default: false)
export DEEPNOTE_ENABLE_EXECUTION_TIMEOUT=true

# Warning threshold in seconds (default: 240 = 4 minutes)
export DEEPNOTE_EXECUTION_WARNING_THRESHOLD=240

# Timeout threshold in seconds (default: 300 = 5 minutes)
export DEEPNOTE_EXECUTION_TIMEOUT_THRESHOLD=300

# Enable automatic interrupt of stuck executions (default: false)
# WARNING: Use with caution! This will send SIGINT to interrupt executions
export DEEPNOTE_EXECUTION_AUTO_INTERRUPT=false
```

**Example Usage**:
```bash
# Enable with default thresholds
export DEEPNOTE_ENABLE_EXECUTION_TIMEOUT=true

# Enable with custom thresholds and auto-interrupt
export DEEPNOTE_ENABLE_EXECUTION_TIMEOUT=true
export DEEPNOTE_EXECUTION_WARNING_THRESHOLD=120  # Warn after 2 minutes
export DEEPNOTE_EXECUTION_TIMEOUT_THRESHOLD=180  # Timeout after 3 minutes
export DEEPNOTE_EXECUTION_AUTO_INTERRUPT=true    # Auto-interrupt
```

## Log Locations

All execution tracking logs are written to the standard Deepnote log location:
- **File**: `$DEEPNOTE_LOG_DIR/helpers.log` (or XDG default)
- **Scraped by**: Loki for centralized logging

## Debugging Stuck Executions

When investigating stuck executions, follow this process:

1. **Check Execution Logs**: Look for `EXEC_START` without matching `EXEC_END`
   ```bash
   grep "EXEC_START\|EXEC_END" $DEEPNOTE_LOG_DIR/helpers.log
   ```

2. **Check ZMQ Message Flow**: Look for message delays or missing messages
   ```bash
   grep "Session.send\|Session.recv" /var/log/jupyter-server.log
   ```

3. **Check for Long Executions**: Look for `LONG_EXECUTION` warnings
   ```bash
   grep "LONG_EXECUTION" $DEEPNOTE_LOG_DIR/helpers.log
   ```

4. **Enable Timeout Monitoring**: If not already enabled, set environment variables and restart kernel

## Implementation Details

### Execution Tracking Flow

1. User executes a cell
2. IPython fires `pre_execute` event → `ExecutionTracker.on_pre_execute()`
   - Logs `EXEC_START`
   - Records start time and cell metadata
3. Cell executes...
4. IPython fires `post_execute` event → `ExecutionTracker.on_post_execute()`
   - Calculates duration
   - Logs `EXEC_END` with duration and success status
   - Publishes metadata to webapp

### Timeout Monitoring Flow (if enabled)

1. `pre_execute` event → `ExecutionTimeoutMonitor.on_pre_execute()`
   - Starts warning timer (default: 240s)
   - Starts timeout timer (default: 300s) if auto-interrupt enabled
2. If warning timer fires → `_send_warning()`
   - Logs `LONG_EXECUTION` warning
   - Reports to webapp
3. If timeout timer fires → `_interrupt_execution()`
   - Logs `TIMEOUT_INTERRUPT` error
   - Reports to webapp
   - Sends SIGINT to kernel (if auto-interrupt enabled)
4. `post_execute` event → cancels all timers

## Files Modified/Created

### New Files
- `deepnote_toolkit/execution_tracking.py` - Execution tracking implementation
- `deepnote_toolkit/execution_timeout.py` - Timeout monitoring implementation
- `docs/EXECUTION_TRACKING.md` - This documentation

### Modified Files
- `deepnote_toolkit/runtime_initialization.py` - Added tracking and timeout setup
- `deepnote_toolkit/ipython_utils.py` - Added execution metadata publishing
- `deepnote_core/resources/jupyter/jupyter_server_config.py` - Enhanced logging config

## Testing

To test the execution tracking:

1. Start a Jupyter kernel with the toolkit
2. Execute a simple cell:
   ```python
   print("Hello, world!")
   ```
3. Check logs for execution tracking:
   ```bash
   tail -f $DEEPNOTE_LOG_DIR/helpers.log | grep "EXEC_"
   ```
4. You should see:
   ```
   EXEC_START | count=1 | cell_id=... | preview=print("Hello, world!")
   EXEC_END | count=1 | duration=0.01s | success=True
   ```

To test timeout monitoring:

1. Enable timeout monitoring:
   ```bash
   export DEEPNOTE_ENABLE_EXECUTION_TIMEOUT=true
   export DEEPNOTE_EXECUTION_WARNING_THRESHOLD=5  # 5 seconds for testing
   ```
2. Restart kernel
3. Execute a long-running cell:
   ```python
   import time
   time.sleep(10)
   ```
4. After 5 seconds, you should see a `LONG_EXECUTION` warning in the logs

## Future Enhancements

Potential improvements to consider:

1. **Kernel Heartbeat Monitoring**: Monitor ZMQ heartbeat channel to detect dead kernels
2. **Message Queue Metrics**: Track IOPub message queue depth to detect backpressure
3. **Execution History**: Store execution history in database for analytics
4. **WebApp Integration**: Display execution metrics in real-time in the UI
5. **Adaptive Timeouts**: Adjust timeout thresholds based on historical execution patterns
6. **Execution Profiling**: Integrate with Python profilers for detailed performance analysis

## References

- Jupyter Client Protocol: https://jupyter-client.readthedocs.io/en/stable/messaging.html
- IPython Events: https://ipython.readthedocs.io/en/stable/config/callbacks.html
- ZMQ Guide: https://zguide.zeromq.org/
