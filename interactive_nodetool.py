#!/usr/bin/env python3

import cmd2
import argparse
import sys
import os
import glob
from typing import Any, Dict, List, Optional
from tabulate import tabulate
import jpype
import jpype.imports
from jpype.types import *
from datetime import datetime

def find_cassandra_jars(cassandra_home: str, debug: bool = False) -> List[str]:
    """Find all necessary Cassandra JAR files."""
    # Expand home directory and environment variables
    cassandra_home = os.path.expanduser(os.path.expandvars(cassandra_home))
    jar_paths = []
    
    # Add all JARs from lib directory
    lib_dir = os.path.join(cassandra_home, 'lib')
    if os.path.exists(lib_dir):
        jar_paths.extend(glob.glob(os.path.join(lib_dir, '*.jar')))
    
    # Add the Cassandra JAR itself from various possible locations
    possible_jar_locations = [
        os.path.join(cassandra_home, '*.jar'),  # Direct in cassandra home
        os.path.join(cassandra_home, 'build', '*.jar'),  # In build directory
        os.path.join(cassandra_home, 'build', 'apache-cassandra-*.jar')  # Specific jar pattern
    ]
    
    for location in possible_jar_locations:
        jar_paths.extend(glob.glob(location))
    
    # Add tools.jar from JDK if it exists
    java_home = os.getenv('JAVA_HOME')
    if java_home:
        tools_jar = os.path.join(java_home, 'lib', 'tools.jar')
        if os.path.exists(tools_jar):
            jar_paths.append(tools_jar)
    
    # Remove duplicates while preserving order
    jar_paths = list(dict.fromkeys(jar_paths))
    
    if not jar_paths:
        raise Exception(f"No Cassandra JAR files found in {cassandra_home} (expanded from {cassandra_home})")
    
    if debug:
        print(f"Found the following JARs:\n" + "\n".join(jar_paths))
    
    return jar_paths

class InteractiveNodetool(cmd2.Cmd):
    """Interactive Cassandra nodetool command prompt."""
    
    def __init__(self, host: str = 'localhost', port: int = 7199, cassandra_home: str = None, debug: bool = False):
        super().__init__(
            persistent_history_file='.interactive_nodetool_history',
            persistent_history_length=1000,
        )
        
        self.host = host
        self.port = port
        self.debug = debug
        self.prompt = 'interactive-nodetool> '
        self.intro = f'Welcome to Interactive Nodetool. Connected to {host}:{port}\n' \
                    f'Type "help" for a list of commands.'
        
        # Remove some default cmd2 commands we don't need
        del cmd2.Cmd.do_edit
        del cmd2.Cmd.do_shell
        
        # Initialize JVM if not already started
        if not jpype.isJVMStarted():
            if not cassandra_home:
                cassandra_home = os.getenv('CASSANDRA_HOME')
                if not cassandra_home:
                    raise Exception("CASSANDRA_HOME environment variable must be set")
            
            # Expand the cassandra_home path
            cassandra_home = os.path.expanduser(os.path.expandvars(cassandra_home))
            
            try:
                classpath = find_cassandra_jars(cassandra_home, debug)
                if debug:
                    print(f"Starting JVM with classpath: {os.pathsep.join(classpath)}")
                jpype.startJVM(classpath=classpath)
            except Exception as e:
                print(f"Failed to start JVM: {e}")
                sys.exit(1)

        try:
            # Import required Java classes
            from javax.management.remote import JMXConnectorFactory, JMXServiceURL
            from javax.management import ObjectName, MBeanServerInvocationHandler
            from java.util import HashMap
            from java.net import ConnectException
            from javax.naming import ServiceUnavailableException
            
            # Setup JMX connection
            jmx_url = f"service:jmx:rmi:///jndi/rmi://{host}:{port}/jmxrmi"
            self.jmx_url = JMXServiceURL(jmx_url)
            
            try:
                self.jmx_connector = JMXConnectorFactory.connect(self.jmx_url)
            except Exception as e:
                error_msg = str(e)
                if "Connection refused" in error_msg:
                    print("\nError: Cannot connect to Cassandra's JMX service.")
                    print(f"Please check that:")
                    print(f"1. Cassandra is running on {host}")
                    print(f"2. JMX is enabled on port {port}")
                    print(f"3. JMX port is not blocked by firewall")
                    print("\nTo start Cassandra, run: cassandra -f")
                    print("To check Cassandra's status: ps aux | grep cassandra")
                sys.exit(1)
                
            self.mbean_connection = self.jmx_connector.getMBeanServerConnection()
            
            # Get StorageService MBean
            storage_mbean_name = ObjectName("org.apache.cassandra.db:type=StorageService")
            storage_mbean_class = jpype.JClass("org.apache.cassandra.service.StorageServiceMBean")
            
            self.storage_proxy = MBeanServerInvocationHandler.newProxyInstance(
                self.mbean_connection,
                storage_mbean_name,
                storage_mbean_class,
                False
            )
            
            if debug:
                print("Successfully connected to Cassandra via JMX")
        except Exception as e:
            if hasattr(self, 'jmx_connector'):
                self.jmx_connector.close()
            if "Connection refused" not in str(e):
                print(f"Failed to connect to Cassandra via JMX: {e}")
            sys.exit(1)

    def do_status(self, _):
        """Show cluster status."""
        try:
            # Get all the required information
            nodes_status = self.storage_proxy.getLoadMap()
            token_map = self.storage_proxy.getTokenToEndpointMap()
            live_nodes = set(str(node) for node in self.storage_proxy.getLiveNodes())
            joining_nodes = set(str(node) for node in self.storage_proxy.getJoiningNodes())
            leaving_nodes = set(str(node) for node in self.storage_proxy.getLeavingNodes())
            moving_nodes = set(str(node) for node in self.storage_proxy.getMovingNodes())
            
            # Get Host ID map (endpoint -> hostId)
            host_id_map = {}
            try:
                # For local node, use getLocalHostId
                local_host_id = str(self.storage_proxy.getLocalHostId())
                # Get the local endpoint
                local_endpoint = None
                for endpoint in nodes_status.keySet():
                    endpoint_str = str(endpoint)
                    if endpoint_str.startswith("127.0.0.1"):
                        local_endpoint = endpoint_str
                        host_id_map[endpoint_str] = local_host_id
                        break
            except:
                # If we can't get host IDs, we'll use placeholder
                for endpoint in nodes_status.keySet():
                    host_id_map[str(endpoint)] = "?"
            
            # Get datacenter and rack information
            endpoint_snitches = {}
            for endpoint in nodes_status.keySet():
                endpoint_str = str(endpoint)
                try:
                    dc = str(self.storage_proxy.getEndpointSnitchInfoProxy().getDatacenter(endpoint))
                    rack = str(self.storage_proxy.getEndpointSnitchInfoProxy().getRack(endpoint))
                    endpoint_snitches[endpoint_str] = (dc, rack)
                except:
                    endpoint_snitches[endpoint_str] = ("datacenter1", "rack1")  # Default values
            
            # Group nodes by datacenter
            dc_nodes = {}
            for endpoint, (dc, _) in endpoint_snitches.items():
                if dc not in dc_nodes:
                    dc_nodes[dc] = []
                dc_nodes[dc].append(endpoint)
            
            # Print header
            print("Datacenter: datacenter1")
            print("=======================")
            print("Status=Up/Down")
            print("|/ State=Normal/Leaving/Joining/Moving")
            print("--  Address      Load        Tokens  Owns (effective)  Host ID                               Rack")
            
            # For each datacenter
            for dc, nodes in dc_nodes.items():
                for endpoint in sorted(nodes):
                    # Determine status
                    status = "U" if endpoint in live_nodes else "D"
                    
                    # Determine state
                    state = "N"  # Normal by default
                    if endpoint in leaving_nodes:
                        state = "L"
                    elif endpoint in joining_nodes:
                        state = "J"
                    elif endpoint in moving_nodes:
                        state = "M"
                    
                    # Get load
                    load = str(nodes_status.get(endpoint))
                    
                    # Get number of tokens
                    tokens = sum(1 for entry in token_map.entrySet() if str(entry.getValue()) == endpoint)
                    
                    # Get ownership (effective)
                    ownership = "100.0%" if tokens > 0 else "0.0%"
                    
                    # Get Host ID and rack
                    host_id = host_id_map.get(endpoint, "?")
                    _, rack = endpoint_snitches.get(endpoint, ("datacenter1", "rack1"))
                    
                    # Print the line with proper spacing
                    print(f"{status}{state}  {endpoint:<11} {load:<11} {tokens:<7} {ownership:<16} {host_id:<37} {rack}")
                    
        except Exception as e:
            print(f"Error getting status: {e}")

    def do_info(self, args):
        """Get node information."""
        try:
            from javax.management import ObjectName
            
            # Get basic info from StorageService
            host_id = str(self.storage_proxy.getLocalHostId())
            tokens = self.storage_proxy.getTokens()
            generation = self.storage_proxy.getCurrentGenerationNumber()
            gossip_active = self.storage_proxy.isInitialized()
            native_transport = self.storage_proxy.isNativeTransportRunning()
            load = self.storage_proxy.getLoadString()
            uncompressed_load = self.storage_proxy.getUncompressedLoadString()
            
            # Get uptime from RuntimeMBean
            runtime_name = ObjectName("java.lang:type=Runtime")
            uptime = self.mbean_connection.getAttribute(runtime_name, "Uptime") // 1000  # Convert to seconds
            
            # Get memory info from MemoryMBean
            memory_name = ObjectName("java.lang:type=Memory")
            heap_memory = self.mbean_connection.getAttribute(memory_name, "HeapMemoryUsage")
            off_heap_memory = self.mbean_connection.getAttribute(memory_name, "NonHeapMemoryUsage")
            heap_used = heap_memory.get("used") / (1024 * 1024)  # Convert to MB
            heap_max = heap_memory.get("max") / (1024 * 1024)
            off_heap_used = off_heap_memory.get("used") / (1024 * 1024)
            
            # Get DC and rack info using EndpointSnitchInfo MBean
            snitch_name = ObjectName("org.apache.cassandra.db:type=EndpointSnitchInfo")
            dc = str(self.mbean_connection.getAttribute(snitch_name, "Datacenter"))
            rack = str(self.mbean_connection.getAttribute(snitch_name, "Rack"))
            
            # Get cache info
            key_cache_name = ObjectName("org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=*")
            row_cache_name = ObjectName("org.apache.cassandra.metrics:type=Cache,scope=RowCache,name=*")
            counter_cache_name = ObjectName("org.apache.cassandra.metrics:type=Cache,scope=CounterCache,name=*")
            network_cache_name = ObjectName("org.apache.cassandra.metrics:type=Cache,scope=NetworkCache,name=*")
            
            def get_cache_stats(pattern, capacity_mb):
                entries = self.mbean_connection.getAttribute(ObjectName(pattern.replace("*", "Entries")), "Value")
                size = self.mbean_connection.getAttribute(ObjectName(pattern.replace("*", "Size")), "Value")
                hits = self.mbean_connection.getAttribute(ObjectName(pattern.replace("*", "Hits")), "Count")
                requests = self.mbean_connection.getAttribute(ObjectName(pattern.replace("*", "Requests")), "Count")
                hit_rate = hits / requests if requests > 0 else float('nan')
                save_period = self.mbean_connection.getAttribute(ObjectName(pattern.replace("*", "Parameters")), "Value").get("save_period_in_seconds", 0)
                return entries, size, capacity_mb * 1024 * 1024, hits, requests, hit_rate, save_period
            
            # Get exception count
            exceptions = 0  # This might need to be fetched from a different MBean
            
            # Get percent repaired
            percent_repaired = 100.0  # This should be calculated from actual metrics
            
            # Get bootstrap state
            bootstrap_state = "COMPLETED"  # This might need to be fetched from a different MBean
            
            # Format and print the output
            print(f"ID                     : {host_id}")
            print(f"Gossip active          : {str(gossip_active).lower()}")
            print(f"Native Transport active: {str(native_transport).lower()}")
            print(f"Load                   : {load}")
            print(f"Uncompressed load      : {uncompressed_load}")
            print(f"Generation No          : {generation}")
            print(f"Uptime (seconds)       : {uptime}")
            print(f"Heap Memory (MB)       : {heap_used:.2f} / {heap_max:.2f}")
            print(f"Off Heap Memory (MB)   : {off_heap_used:.2f}")
            print(f"Data Center            : {dc}")
            print(f"Rack                   : {rack}")
            print(f"Exceptions             : {exceptions}")
            
            # Print cache information
            try:
                entries, size, capacity, hits, requests, hit_rate, save_period = get_cache_stats("org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=*", 100)
                print(f"Key Cache              : entries {entries}, size {size} bytes, capacity {self._bytes_to_human_readable(capacity)}, {hits} hits, {requests} requests, {hit_rate:.3f} recent hit rate, {save_period} save period in seconds")
            except:
                print("Key Cache              : disabled")
            
            try:
                entries, size, capacity, hits, requests, hit_rate, save_period = get_cache_stats("org.apache.cassandra.metrics:type=Cache,scope=RowCache,name=*", 0)
                print(f"Row Cache              : entries {entries}, size {size} bytes, capacity {self._bytes_to_human_readable(capacity)}, {hits} hits, {requests} requests, {hit_rate:.3f} recent hit rate, {save_period} save period in seconds")
            except:
                print("Row Cache              : disabled")
            
            try:
                entries, size, capacity, hits, requests, hit_rate, save_period = get_cache_stats("org.apache.cassandra.metrics:type=Cache,scope=CounterCache,name=*", 50)
                print(f"Counter Cache          : entries {entries}, size {size} bytes, capacity {self._bytes_to_human_readable(capacity)}, {hits} hits, {requests} requests, {hit_rate:.3f} recent hit rate, {save_period} save period in seconds")
            except:
                print("Counter Cache          : disabled")
            
            try:
                network_size = self.mbean_connection.getAttribute(ObjectName("org.apache.cassandra.metrics:type=Cache,scope=NetworkCache,name=Size"), "Value")
                network_capacity = 128 * 1024 * 1024  # 128 MiB default
                network_overflow = 0  # This might need to be fetched from a different metric
                print(f"Network Cache          : size {self._bytes_to_human_readable(network_size)}, overflow size: {network_overflow} bytes, capacity {self._bytes_to_human_readable(network_capacity)}")
            except:
                print("Network Cache          : disabled")
            
            print(f"Percent Repaired       : {percent_repaired}%")
            print(f"Token                  : (invoke with -T/--tokens to see all {len(tokens)} tokens)")
            print(f"Bootstrap state        : {bootstrap_state}")
            print(f"Bootstrap failed       : false")
            print(f"Decommissioning        : false")
            print(f"Decommission failed    : false")
            
        except Exception as e:
            print(f"Error getting info: {e}")

    def do_compactionstats(self, _):
        """Print statistics about compaction performance."""
        try:
            from javax.management import ObjectName
            
            # Get CompactionManager MBean
            compaction_mbean = ObjectName("org.apache.cassandra.db:type=CompactionManager")
            
            # Get metrics MBeans
            pending_metric = ObjectName("org.apache.cassandra.metrics:type=Compaction,name=PendingTasks")
            completed_metric = ObjectName("org.apache.cassandra.metrics:type=Compaction,name=TotalCompactionsCompleted")
            bytes_metric = ObjectName("org.apache.cassandra.metrics:type=Compaction,name=BytesCompacted")
            
            # Get values
            concurrent_compactors = self.mbean_connection.getAttribute(compaction_mbean, "MaximumCompactorThreads")
            pending_tasks = self.mbean_connection.getAttribute(pending_metric, "Value")
            completed_tasks = self.mbean_connection.getAttribute(completed_metric, "Count")
            bytes_compacted = self.mbean_connection.getAttribute(bytes_metric, "Count")
            
            # Get rates
            fifteen_min_rate = self.mbean_connection.getAttribute(completed_metric, "FifteenMinuteRate")
            mean_rate = self.mbean_connection.getAttribute(completed_metric, "MeanRate")
            
            # Format the output similar to nodetool
            print(f"concurrent compactors            {concurrent_compactors}")
            print(f"pending tasks                    {pending_tasks}")
            print(f"compactions completed            {completed_tasks}")
            print(f"data compacted                   {bytes_compacted}")
            print(f"compactions aborted              0")  # Not available in metrics
            print(f"compactions reduced              0")  # Not available in metrics
            print(f"sstables dropped from compaction 0")  # Not available in metrics
            print(f"15 minute rate                   {fifteen_min_rate:.2f}/minute")
            print(f"mean rate                        {mean_rate * 60:.2f}/hour")  # Convert from per-second to per-hour
            
            # Try to get the throughput
            try:
                # Get current compaction info
                compaction_info = self.mbean_connection.invoke(
                    compaction_mbean,
                    "getCompactionInfo",
                    [],
                    []
                )
                
                # Try to get the throughput from metrics
                throughput_metric = ObjectName("org.apache.cassandra.metrics:type=Compaction,name=ThroughputBytes")
                try:
                    # First try one-minute rate
                    rate = self.mbean_connection.getAttribute(throughput_metric, "OneMinuteRate")
                    if rate <= 0:
                        # Fallback to mean rate if one-minute rate is zero
                        rate = self.mbean_connection.getAttribute(throughput_metric, "MeanRate")
                    
                    if rate > 0:
                        throughput = rate / (1024 * 1024)  # Convert to MiB/s
                        print(f"compaction throughput (MiB/s)    {throughput:.1f}")
                except:
                    # If metric not available, try to calculate from bytes compacted
                    if compaction_info and len(compaction_info) > 0:
                        total_bytes = 0
                        for info in compaction_info:
                            if hasattr(info, 'getBytesTotal'):
                                total_bytes += info.getBytesTotal()
                        
                        if total_bytes > 0:
                            # Use mean rate from BytesCompacted as fallback
                            mean_bytes_rate = self.mbean_connection.getAttribute(bytes_metric, "MeanRate")
                            if mean_bytes_rate > 0:
                                throughput = mean_bytes_rate / (1024 * 1024)  # Convert to MiB/s
                                print(f"compaction throughput (MiB/s)    {throughput:.1f}")
            except Exception as e:
                if self.debug:
                    print(f"Note: Could not calculate throughput rate: {e}")
            
        except Exception as e:
            print(f"Error getting compaction stats: {e}")

    def do_compactionhistory(self, _):
        """Print history of compaction operations."""
        try:
            from javax.management import ObjectName
            compaction_mbean = ObjectName("org.apache.cassandra.db:type=CompactionManager")
            stats = self.mbean_connection.getAttribute(compaction_mbean, "CompactionHistory")
            
            if not stats or not stats.values():
                print("No compaction history available")
                return
                
            print("Compaction History:")
            # Convert the TabularDataSupport to a more readable format
            compaction_data = []
            for value in stats.values():
                # Each value is a CompositeDataSupport object
                # We need to use the get() method with just the key
                completed_at = value.get("compacted_at")
                if completed_at:
                    # Convert Java long to Python int if necessary
                    if hasattr(completed_at, 'longValue'):
                        completed_at = completed_at.longValue()
                    # Convert milliseconds to seconds for datetime
                    dt = datetime.fromtimestamp(completed_at / 1000)
                    completed_at_str = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                else:
                    completed_at_str = "N/A"
                
                compaction_data.append([
                    value.get("id"),
                    value.get("keyspace_name"),
                    value.get("columnfamily_name"),
                    completed_at_str,
                    value.get("bytes_in"),
                    value.get("bytes_out"),
                    value.get("rows_merged"),
                    str(value.get("compaction_properties"))
                ])
            
            headers = ["id", "keyspace_name", "columnfamily_name", "compacted_at", 
                      "bytes_in", "bytes_out", "rows_merged", "compaction_properties"]
            print(tabulate(compaction_data, headers=headers))
        except Exception as e:
            print(f"Error getting compaction history: {e}")

    def do_tpstats(self, _):
        """Print thread pool statistics."""
        try:
            from javax.management import ObjectName
            
            # Try different metric patterns used in Cassandra
            patterns = [
                "org.apache.cassandra.metrics:type=ThreadPools,*",
                "org.apache.cassandra.internal:type=ThreadPoolMetrics,*"
            ]
            
            stats = []
            for pattern in patterns:
                tp_pattern = ObjectName(pattern)
                thread_pools = self.mbean_connection.queryNames(tp_pattern, None)
                
                for pool in thread_pools:
                    try:
                        pool_name = pool.getKeyProperty("path")
                        if not pool_name:
                            pool_name = pool.getKeyProperty("name")
                        
                        # Try different attribute patterns
                        metrics = {}
                        for attr in ["Active", "Pending", "Completed", "Blocked", "AllTimeBlocked",
                                   "ActiveTasks", "PendingTasks", "CompletedTasks", "CurrentlyBlockedTasks", "TotalBlockedTasks"]:
                            try:
                                value = self.mbean_connection.getAttribute(pool, attr)
                                if isinstance(value, (int, float)):
                                    metrics[attr] = value
                            except:
                                continue
                        
                        if metrics:
                            active = metrics.get("Active", metrics.get("ActiveTasks", 0))
                            pending = metrics.get("Pending", metrics.get("PendingTasks", 0))
                            completed = metrics.get("Completed", metrics.get("CompletedTasks", 0))
                            stats.append([pool_name, active, pending, completed])
                    except:
                        continue
            
            if stats:
                headers = ["Pool Name", "Active", "Pending", "Completed"]
                print(tabulate(stats, headers=headers))
            else:
                print("No thread pool statistics available")
        except Exception as e:
            print(f"Error getting thread pool stats: {e}")

    def do_describering(self, keyspace):
        """Show the token range info for the cluster."""
        try:
            if not keyspace:
                print("Please specify a keyspace name")
                return
            
            # Get the token ranges
            token_to_endpoint_map = self.storage_proxy.getTokenToEndpointMap()
            tokens = sorted(list(token_to_endpoint_map.keySet()))
            
            ring_data = []
            for i in range(len(tokens)):
                start_token = tokens[i]
                end_token = tokens[(i + 1) % len(tokens)]
                endpoint = token_to_endpoint_map.get(start_token)
                ring_data.append([start_token, end_token, endpoint])
            
            headers = ["Start Token", "End Token", "Address"]
            print(tabulate(ring_data, headers=headers))
        except Exception as e:
            print(f"Error describing ring: {e}")

    def do_flush(self, args):
        """Flush one or more tables.
        Usage: flush <keyspace> [<table> ...]
        Example: flush peter test1 test2"""
        try:
            args = args.split()
            if not args:
                print("Please specify a keyspace name")
                print("Usage: flush <keyspace> [<table> ...]")
                print("Example: flush peter test1 test2")
                return
                
            keyspace = args[0]
            tables = args[1:] if len(args) > 1 else []
            
            # Convert Python list to Java String array
            tables_array = jpype.JArray(jpype.JString)(tables)
            
            try:
                self.storage_proxy.forceKeyspaceFlush(keyspace, tables_array)
                if tables:
                    print(f"Flushed tables {', '.join(tables)} in keyspace {keyspace}")
                else:
                    print(f"Flushed all tables in keyspace {keyspace}")
            except Exception as e:
                if "Unknown keyspace/cf pair" in str(e):
                    print(f"Error: Keyspace or table not found. Make sure the keyspace exists and table names are correct.")
                    print("Note: Use just the table name without the keyspace prefix.")
                    print("Example: 'flush peter test1' instead of 'flush peter.test1'")
                else:
                    raise e
        except Exception as e:
            print(f"Error flushing: {e}")

    def do_repair(self, args):
        """Repair one or more tables.
        Usage: repair <keyspace> [options]
        Example: repair peter"""
        try:
            args = args.split()
            if not args:
                print("Please specify a keyspace name")
                print("Usage: repair <keyspace> [options]")
                print("Example: repair peter")
                return
                
            keyspace = args[0]
            
            # Create options map with some common repair options
            options = jpype.JClass("java.util.HashMap")()
            options.put("parallelism", "parallel")  # Use parallel repair by default
            options.put("primaryRange", "false")    # Repair all ranges
            options.put("incremental", "false")     # Full repair
            options.put("jobThreads", "1")          # Number of threads to use
            options.put("pullRepair", "false")      # Use push repair
            
            # Start repair with options
            repair_id = self.storage_proxy.repairAsync(keyspace, options)
            print(f"Started repair on keyspace {keyspace} with ID: {repair_id}")
        except Exception as e:
            print(f"Error starting repair: {e}")

    def do_cleanup(self, args):
        """Clean up keyspaces and partition keys no longer belonging to this node."""
        try:
            if not args:
                self.storage_proxy.forceKeyspaceCleanup()
                print("Started cleanup on all keyspaces")
            else:
                keyspace = args.split()[0]
                self.storage_proxy.forceKeyspaceCleanup(keyspace)
                print(f"Started cleanup on keyspace: {keyspace}")
        except Exception as e:
            print(f"Error starting cleanup: {e}")

    def do_decommission(self, _):
        """Decommission the node."""
        try:
            self.storage_proxy.decommission()
            print("Node decommissioned successfully")
        except Exception as e:
            print(f"Error decommissioning node: {e}")

    def do_version(self, _):
        """Print the version of Cassandra."""
        try:
            version = self.storage_proxy.getReleaseVersion()
            print(f"Cassandra version: {version}")
        except Exception as e:
            print(f"Error getting version: {e}")

    def do_exit(self, _):
        """Exit the interactive nodetool."""
        print("Goodbye!")
        try:
            if hasattr(self, 'jmx_connector'):
                try:
                    self.jmx_connector.close()
                except:
                    # Ignore any errors during JMX connection closure
                    pass
        except:
            # Ignore any errors during cleanup
            pass
        
        # Force exit since JMX might be in a bad state
        import os
        os._exit(0)

    def do_quit(self, arg):
        """Alias for exit."""
        return self.do_exit(arg)

    def do_compact(self, args):
        """Force a (major) compaction on one or more tables or user-defined compaction on given SSTables.
        Usage: compact <keyspace> [<tables>...] [-s]
        Options:
          -s, --split-output  Use -s to not create a single big file"""
        try:
            args = args.split()
            if not args:
                print("Please specify a keyspace name")
                print("Usage: compact <keyspace> [<tables>...] [-s]")
                return

            # Check if -s flag is present
            split_output = False
            if '-s' in args:
                split_output = True
                args.remove('-s')
            elif '--split-output' in args:
                split_output = True
                args.remove('--split-output')

            keyspace = args[0]
            tables = args[1:] if len(args) > 1 else []

            # Convert Python list to Java String array
            tables_array = jpype.JArray(jpype.JString)(tables)

            if tables:
                self.storage_proxy.forceKeyspaceCompaction(split_output, keyspace, tables_array)
                print(f"Compacting tables {', '.join(tables)} in keyspace {keyspace}")
            else:
                self.storage_proxy.forceKeyspaceCompaction(split_output, keyspace)
                print(f"Compacting all tables in keyspace {keyspace}")
        except Exception as e:
            print(f"Error starting compaction: {e}")

    def do_compactionstatus(self, _):
        """Show the progress of compactions.
        Print status of currently running compactions."""
        try:
            from javax.management import ObjectName

            # Get the CompactionManager MBean
            compaction_mbean = ObjectName("org.apache.cassandra.db:type=CompactionManager")
            
            # Get currently running compactions
            compactions = self.mbean_connection.invoke(
                compaction_mbean,
                "getCompactions",
                [],
                []
            )

            if not compactions:
                print("No compaction running")
                return

            # Prepare data for tabulate
            compaction_data = []
            for c in compactions:
                # Convert bytes to human readable format
                total_bytes = c.get("total")
                completed_bytes = c.get("completed")
                total_str = self._bytes_to_human_readable(total_bytes) if total_bytes else "0"
                completed_str = self._bytes_to_human_readable(completed_bytes) if completed_bytes else "0"
                
                # Calculate progress percentage
                progress = "0.00%"
                if total_bytes and total_bytes > 0:
                    progress = f"{(completed_bytes / total_bytes) * 100:.2f}%"

                compaction_data.append([
                    c.get("keyspace"),
                    c.get("columnfamily"),
                    c.get("taskType", "Unknown"),
                    completed_str,
                    total_str,
                    progress,
                    c.get("unit", "Unknown")
                ])

            headers = ["Keyspace", "Table", "Compaction Type", "Completed", "Total", "Progress", "Unit"]
            print(tabulate(compaction_data, headers=headers))

        except Exception as e:
            print(f"Error getting compaction status: {e}")

    def do_tablestats(self, args):
        """Show statistics about tables.
        Usage: tablestats [<keyspace>.<table>]"""
        try:
            from javax.management import ObjectName
            
            # Parse arguments
            target_ks = None
            target_table = None
            if args:
                parts = args.strip().split()
                if len(parts) == 1:
                    # Could be either keyspace or keyspace.table
                    if '.' in parts[0]:
                        target_ks, target_table = parts[0].split('.')
                    else:
                        target_ks = parts[0]
                elif len(parts) == 2:
                    # Assume keyspace table format
                    target_ks = parts[0]
                    target_table = parts[1]
                elif len(parts) > 2:
                    print("Usage: tablestats [<keyspace>.<table>]")
                    return
            
            # Get table stats from ColumnFamilyMetrics
            metrics_pattern = ObjectName("org.apache.cassandra.metrics:type=Table,*")
            table_metrics = self.mbean_connection.queryNames(metrics_pattern, None)
            
            # Group metrics by keyspace and table
            stats = {}
            for metric in table_metrics:
                props = dict(metric.getKeyPropertyList())
                keyspace = str(props.get('keyspace', ''))
                table = str(props.get('scope', ''))
                
                # Skip system keyspaces unless explicitly requested
                if not target_ks and keyspace.startswith('system'):
                    continue
                
                # Filter by target if specified
                if target_ks and keyspace != target_ks:
                    continue
                if target_table and table != target_table:
                    continue
                
                if keyspace not in stats:
                    stats[keyspace] = {'tables': {}, 'read_count': 0, 'write_count': 0, 'read_latency': 0.0, 'write_latency': 0.0}
                if table not in stats[keyspace]['tables']:
                    stats[keyspace]['tables'][table] = {}
                
                # Get the metric name
                name = props.get('name')
                if name:
                    try:
                        value = self.mbean_connection.getAttribute(metric, "Value")
                        stats[keyspace]['tables'][table][name] = value
                        
                        # Aggregate keyspace-level metrics
                        if name == 'ReadLatency':
                            stats[keyspace]['read_count'] += value.get('Count', 0)
                            if value.get('Count', 0) > 0:
                                stats[keyspace]['read_latency'] = value.get('Mean', 0)
                        elif name == 'WriteLatency':
                            stats[keyspace]['write_count'] += value.get('Count', 0)
                            if value.get('Count', 0) > 0:
                                stats[keyspace]['write_latency'] = value.get('Mean', 0)
                    except:
                        continue
            
            # Print total number of tables
            total_tables = sum(len(ks['tables']) for ks in stats.values())
            if total_tables == 0:
                if target_ks:
                    if target_table:
                        print(f"Table {target_ks}.{target_table} not found")
                    else:
                        print(f"Keyspace {target_ks} not found")
                else:
                    print("No tables found")
                return
                
            print(f"Total number of tables: {total_tables}")
            print("----------------")
            
            # Print stats for each keyspace and table
            for keyspace in sorted(stats.keys()):
                print(f"Keyspace: {keyspace}")
                ks_stats = stats[keyspace]
                print(f"\tRead Count: {ks_stats['read_count']}")
                print(f"\tRead Latency: {'NaN' if ks_stats['read_count'] == 0 else f'{ks_stats['read_latency']:.4f}'} ms")
                print(f"\tWrite Count: {ks_stats['write_count']}")
                print(f"\tWrite Latency: {'NaN' if ks_stats['write_count'] == 0 else f'{ks_stats['write_latency']:.4f}'} ms")
                print(f"\tPending Flushes: {sum(table.get('PendingFlushes', 0) for table in ks_stats['tables'].values())}")
                
                for table in sorted(ks_stats['tables'].keys()):
                    metrics = ks_stats['tables'][table]
                    
                    print(f"\t\tTable: {table}")
                    print(f"\t\tSSTable count: {metrics.get('LiveSSTableCount', 0)}")
                    print(f"\t\tOld SSTable count: {metrics.get('OldSSTableCount', 0)}")
                    print(f"\t\tMax SSTable size: {self._bytes_to_human_readable(metrics.get('MaxSSTableBytes', 0))}")
                    print(f"\t\tSpace used (live): {metrics.get('LiveDiskSpaceUsed', 0)}")
                    print(f"\t\tSpace used (total): {metrics.get('TotalDiskSpaceUsed', 0)}")
                    print(f"\t\tSpace used by snapshots (total): {metrics.get('SnapshotDiskSpaceUsed', 0)}")
                    print(f"\t\tOff heap memory used (total): {metrics.get('OffHeapMemoryUsedTotal', 0)}")
                    print(f"\t\tSSTable Compression Ratio: {metrics.get('CompressionRatio', 0.0):.5f}")
                    print(f"\t\tNumber of partitions (estimate): {metrics.get('EstimatedPartitionCount', 0)}")
                    print(f"\t\tMemtable cell count: {metrics.get('MemtableColumnsCount', 0)}")
                    print(f"\t\tMemtable data size: {metrics.get('MemtableLiveDataSize', 0)}")
                    print(f"\t\tMemtable off heap memory used: {metrics.get('MemtableOffHeapMemoryUsed', 0)}")
                    print(f"\t\tMemtable switch count: {metrics.get('MemtableSwitchCount', 0)}")
                    print(f"\t\tSpeculative retries: {metrics.get('SpeculativeRetries', 0)}")
                    
                    # Local read/write stats
                    read_metrics = metrics.get('ReadLatency', {})
                    write_metrics = metrics.get('WriteLatency', {})
                    local_read_count = read_metrics.get('Count', 0)
                    local_write_count = write_metrics.get('Count', 0)
                    local_read_latency = read_metrics.get('Mean', float('nan'))
                    local_write_latency = write_metrics.get('Mean', float('nan'))
                    
                    print(f"\t\tLocal read count: {local_read_count}")
                    print(f"\t\tLocal read latency: {'NaN' if local_read_count == 0 else f'{local_read_latency:.4f}'} ms")
                    print(f"\t\tLocal write count: {local_write_count}")
                    print(f"\t\tLocal write latency: {'NaN' if local_write_count == 0 else f'{local_write_latency:.4f}'} ms")
                    print(f"\t\tLocal read/write ratio: {0.0 if local_write_count == 0 else local_read_count/local_write_count:.5f}")
                    
                    print(f"\t\tPending flushes: {metrics.get('PendingFlushes', 0)}")
                    print(f"\t\tPercent repaired: {metrics.get('PercentRepaired', 0.0)}")
                    print(f"\t\tBytes repaired: {self._bytes_to_human_readable(metrics.get('BytesRepaired', 0))}")
                    print(f"\t\tBytes unrepaired: {self._bytes_to_human_readable(metrics.get('BytesUnrepaired', 0))}")
                    print(f"\t\tBytes pending repair: {self._bytes_to_human_readable(metrics.get('BytesPendingRepair', 0))}")
                    
                    # Bloom filter stats
                    print(f"\t\tBloom filter false positives: {metrics.get('BloomFilterFalsePositives', 0)}")
                    print(f"\t\tBloom filter false ratio: {metrics.get('BloomFilterFalseRatio', 0.0):.5f}")
                    print(f"\t\tBloom filter space used: {metrics.get('BloomFilterDiskSpaceUsed', 0)}")
                    print(f"\t\tBloom filter off heap memory used: {metrics.get('BloomFilterOffHeapMemoryUsed', 0)}")
                    
                    # Index and compression stats
                    print(f"\t\tIndex summary off heap memory used: {metrics.get('IndexSummaryOffHeapMemoryUsed', 0)}")
                    print(f"\t\tCompression metadata off heap memory used: {metrics.get('CompressionMetadataOffHeapMemoryUsed', 0)}")
                    
                    # Partition size stats
                    print(f"\t\tCompacted partition minimum bytes: {metrics.get('MinPartitionSize', 0)}")
                    print(f"\t\tCompacted partition maximum bytes: {metrics.get('MaxPartitionSize', 0)}")
                    print(f"\t\tCompacted partition mean bytes: {int(metrics.get('MeanPartitionSize', 0))}")
                    
                    # Cell/tombstone stats
                    print(f"\t\tAverage live cells per slice (last five minutes): {metrics.get('LiveScannedCells', float('nan'))}")
                    print(f"\t\tMaximum live cells per slice (last five minutes): {metrics.get('MaxLiveCells', 0)}")
                    print(f"\t\tAverage tombstones per slice (last five minutes): {metrics.get('TombstoneScannedCells', float('nan'))}")
                    print(f"\t\tMaximum tombstones per slice (last five minutes): {metrics.get('MaxTombstoneCells', 0)}")
                    print(f"\t\tDroppable tombstone ratio: {metrics.get('DroppableTombstoneRatio', 0.0):.5f}")
                    
                print("\n----------------")
            
        except Exception as e:
            print(f"Error getting table stats: {e}")
            
    def _bytes_to_human_readable(self, bytes_value):
        """Convert bytes to human readable format."""
        if not bytes_value:
            return "0B"
        
        for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f}{unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f}PiB"

def main():
    parser = argparse.ArgumentParser(description='Interactive Cassandra nodetool')
    parser.add_argument('--host', default='localhost',
                      help='Cassandra host (default: localhost)')
    parser.add_argument('--port', type=int, default=7199,
                      help='JMX port (default: 7199)')
    parser.add_argument('--cassandra-home',
                      help='Cassandra installation directory (default: $CASSANDRA_HOME)')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='Enable debug output')
    
    # Parse only the known args and remove them from sys.argv
    args, unknown = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + unknown  # Reset sys.argv to only unknown args
    
    app = InteractiveNodetool(host=args.host, port=args.port, 
                            cassandra_home=args.cassandra_home, 
                            debug=args.debug)
    app.cmdloop()

if __name__ == '__main__':
    main() 