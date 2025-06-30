# Interactive Nodetool

An interactive command-line tool for managing Apache Cassandra and DataStax Enterprise (DSE) clusters, built with Python and JPype.

## Features

- **Interactive shell** with command history and tab completion
- **Version compatibility** for Cassandra 4.0+, 4.1+, 5.0+ and DSE 5.1+, 6.8+, 6.9+
- **Automatic version detection** and feature compatibility
- **SSL/TLS support** for secure JMX connections
- **Output redirection** for command results
- **DSE-specific features** detection and information
- **Comprehensive compatibility analysis** and warnings

## Supported Versions

### Apache Cassandra
- **4.0.x** - Full support with modern gossip states and endpoint ports
- **4.1.x** - Full support with all 4.0 features
- **5.0.x** - Full support with CIDR filtering and disk usage metrics

### DataStax Enterprise (DSE)
- **5.1.x** - Full support with DSE-specific features
- **6.8.x** - Full support with Analytics, Search, and Graph
- **6.9.x** - Full support with all DSE features

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set CASSANDRA_HOME environment variable:**
   ```bash
   export CASSANDRA_HOME=/path/to/cassandra
   ```

3. **Run the tool:**
   ```bash
   python3 interactive_nodetool.py
   ```

## Usage

### Basic Usage

```bash
# Connect to local Cassandra instance
python3 interactive_nodetool.py

# Connect to remote instance
python3 interactive_nodetool.py --host 192.168.1.100 --port 7199

# Execute single command and exit
python3 interactive_nodetool.py -c status

# Save command output to file
python3 interactive_nodetool.py -c status -o /tmp/output
```

### SSL/TLS Connection

```bash
# Connect with SSL
python3 interactive_nodetool.py --ssl --truststore /path/to/truststore.jks

# Connect with SSL and authentication
python3 interactive_nodetool.py --ssl --truststore /path/to/truststore.jks \
    --keystore /path/to/keystore.jks --username admin
```

### Configuration File

Create a `config.txt` file:
```
host = 192.168.1.100
port = 7199
ssl = true
truststore = /path/to/truststore.jks
truststore-password = mypassword
username = admin
password = mypassword
```

Then use:
```bash
python3 interactive_nodetool.py -f config.txt
```

## Commands

### Standard Nodetool Commands
- `status` / `s` - Show cluster status
- `info` / `i` - Get node information
- `version` / `v` - Print Cassandra version
- `gossipinfo` - Show gossip information
- `describecluster` - Show cluster information
- `tpstats` / `tps` - Print thread pool statistics
- `compactionstats` - Print compaction statistics
- `tablestats` - Show table statistics
- `netstats` / `n` - Print network information

### Version Compatibility Commands
- `versioninfo` - Show detailed version and compatibility information
- `compatibility` - Show compatibility warnings and recommendations
- `dseinfo` - Show DSE-specific information (DSE only)

### Utility Commands
- `loop` - Run commands in a loop with wait time
- `help` - Show available commands
- `exit` / `quit` - Exit the tool

## Version Compatibility

The tool automatically detects your Cassandra/DSE version and adjusts its behavior accordingly:

### Version Detection
- Automatically detects Cassandra vs DSE
- Parses version numbers (major.minor.patch)
- Identifies available features and deprecated methods

### Feature Compatibility
- **Cassandra 4.0+**: Modern gossip states, endpoint ports, SSL debugging
- **Cassandra 5.0+**: CIDR filtering, disk usage metrics, additional application states
- **DSE**: Analytics, Search, Graph, and Insights detection

### Compatibility Warnings
The `compatibility` command shows:
- Deprecated methods in your version
- Missing features compared to newer versions
- Version-specific recommendations
- Potential compatibility issues

## Examples

### Check Version Compatibility
```
interactive-nodetool> versioninfo
Detected Version: 5.0.4
Is DSE: False
Major: 5
Minor: 0
Patch: 4

Compatibility Features:
  Supports ports in endpoints: True
  SSL debugging supported: True
  Audit logging supported: True
  Full query logging supported: True
  CIDR filtering supported: True
```

### DSE Information
```
interactive-nodetool> dseinfo
DSE Version: DSE 6.8.0
Based on Cassandra: 6.8.0

DSE Analytics:
  Status: Available

DSE Search:
  Status: Available

DSE Graph:
  Status: Available
```

### Compatibility Analysis
```
interactive-nodetool> compatibility
Compatibility Analysis for 5.0.4
==================================================

📋 RECOMMENDATIONS:
  - This is a recent version with all modern features
  - CIDR filtering and disk usage metrics are available

✅ COMPATIBILITY STATUS:
  - Fully compatible with this tool
```

## Troubleshooting

### Connection Issues
- Ensure Cassandra is running and JMX is enabled
- Check firewall settings for JMX port (default: 7199)
- Verify SSL configuration if using SSL

### Version Detection Issues
- Run `versioninfo` to see detected version
- Use `compatibility` to check for issues
- Enable debug mode with `--debug` for detailed information

### DSE-Specific Issues
- Use `dseinfo` to check DSE component availability
- Ensure DSE-specific JARs are in the classpath
- Check DSE documentation for version-specific features

## Development

### Running Tests
```bash
# Test version detection logic
python3 test_version_detection.py
```

### Adding New Commands
1. Add a `do_commandname` method to the `InteractiveNodetool` class
2. Use `self.version_compat` for version-specific behavior
3. Add appropriate error handling and debug output

### Version Compatibility
- Use `self.cassandra_version` to check version
- Use `self.version_compat` for version-specific methods
- Test with multiple Cassandra/DSE versions

## License

This project is licensed under the Apache License 2.0.
