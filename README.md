# Interactive Nodetool

An interactive command-line interface for Apache Cassandra's nodetool functionality. This tool provides a shell-like interface for executing nodetool commands, with features like command history, tab completion, and built-in help. Unlike the standard nodetool command, this implementation uses JMX directly, maintaining a single JVM instance for better performance.

## Features

- Interactive prompt for nodetool commands
- Command history and recall
- Tab completion for commands and options
- Built-in help and documentation
- Direct JMX connection to Cassandra nodes (no subprocess calls)
- Single JVM instance for all operations
- Efficient command execution
- Command looping with configurable wait times
- Direct command execution from command line

## Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Ensure you have a Java Runtime Environment (JRE) installed
4. Set the CASSANDRA_HOME environment variable to point to your Cassandra installation:
   ```bash
   export CASSANDRA_HOME=/path/to/cassandra
   ```

## Usage

Run the interactive shell:
```bash
python interactive_nodetool.py [--host HOST] [--port PORT] [--cassandra-home CASSANDRA_HOME]
```

Execute commands directly:
```bash
# Execute a command and exit
python interactive_nodetool.py -c "status"
python interactive_nodetool.py --command "info"

# Execute a command and enter interactive mode
python interactive_nodetool.py -C "status"
python interactive_nodetool.py --interactivecommand "info"

# Commands with arguments are also supported
python interactive_nodetool.py -c "loop 3 (info 'wait 2')"

# Save command output to a file (requires -c/--command)
python interactive_nodetool.py -c "status" -o output_dir
python interactive_nodetool.py -c "loop 3 (info status 'wait 2')" -o logs
```

The `-o/--output` option:
- Must be used with `-c/--command`
- Creates a directory if it doesn't exist
- Saves command output to files named `nodetool-<command>-<datetime>.out`
- For loop commands, creates separate files for each command in the loop
- Writes output both to the file and to the terminal

Once in the shell, you can run nodetool commands directly without the "nodetool" prefix:

```
interactive-nodetool> status
interactive-nodetool> info
interactive-nodetool> help
```

### Command Looping

You can run commands in a loop with a configurable wait time between iterations using the `loop` command:

```
loop <iterations> (<command1> <command2> ... 'wait <seconds>')
```

Examples:
```
# Run info and status 3 times with 2 second wait between iterations
interactive-nodetool> loop 3 (info status 'wait 2')

# Monitor compaction status every 5 seconds, 10 times
interactive-nodetool> loop 10 (compactionstatus 'wait 5')

# Check node status every second, 60 times (1 minute monitoring)
interactive-nodetool> loop 60 (status 'wait 1')
```

Each iteration will be timestamped and separated by clear markers for easy reading. You can interrupt the loop at any time using Ctrl+C.

## Configuration

By default, the tool connects to localhost:7199. You can specify different connection parameters when starting the tool:

```bash
python interactive_nodetool.py --host <host> --port <port> --cassandra-home <cassandra_home>
```

## Requirements

- Python 3.8+
- Java Runtime Environment (JRE)
- Running Cassandra instance
- JMX access to Cassandra node
- Cassandra installation (for accessing JMX classes)

## Implementation Details

This tool uses JMX (Java Management Extensions) to communicate directly with Cassandra, maintaining a single JVM instance throughout the session. This approach is more efficient than the traditional nodetool command, which starts a new JVM for each command execution.

The implementation:
1. Starts a JVM using JPype
2. Establishes a JMX connection to Cassandra
3. Uses Cassandra's MBeans directly for operations
4. Maintains the JVM and connection throughout the session

## Notes

This has currently only been tested against a local install of Cassanra 5.0.4.
This is not a wrapper for the native nodetool commands and is performing the underlying JMX calls and formatting the output in a similar way to nodetool. Therefore need to add each nodetool command over time.
