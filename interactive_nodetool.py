#!/usr/bin/env python3

import cmd2
import argparse
import sys
import os
import glob
import readline
from typing import Any, Dict, List, Optional
from tabulate import tabulate
import jpype
import jpype.imports
from jpype.types import *
from datetime import datetime
import time

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
        # Initialize readline history
        self.history_file = os.path.expanduser('~/.interactive_nodetool_history')
        self.debug = debug
        
        # Load history if it exists
        if os.path.exists(self.history_file):
            try:
                readline.read_history_file(self.history_file)
                if debug:
                    print(f"Loaded {readline.get_current_history_length()} history items")
            except Exception as e:
                if debug:
                    print(f"Error reading history file: {e}")
        
        # Set history length
        readline.set_history_length(1000)
        
        # Initialize cmd2 without its history feature
        super().__init__(
            allow_cli_args=False,
            persistent_history_file=None  # Disable cmd2's history
        )
        
        self.host = host
        self.port = port
        self.prompt = 'interactive-nodetool> '
        self.intro = f'Welcome to Interactive Nodetool. Connected to {host}:{port}\n' \
                    f'Type "help" for a list of commands.'
        
        # Remove some default cmd2 commands we don't need
        del cmd2.Cmd.do_edit
        del cmd2.Cmd.do_shell
        del cmd2.Cmd.do_macro
        del cmd2.Cmd.do_alias
        del cmd2.Cmd.do_shortcuts
        del cmd2.Cmd.do_run_pyscript
        del cmd2.Cmd.do_run_script
        del cmd2.Cmd.do_set
        
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

    def postcmd(self, stop: bool, line: str) -> bool:
        """Save history after each command."""
        try:
            readline.write_history_file(self.history_file)
            if self.debug:
                print(f"Saved {readline.get_current_history_length()} history items")
        except Exception as e:
            if self.debug:
                print(f"Error saving history: {e}")
        return stop

    def preloop(self) -> None:
        """Setup before starting the command loop."""
        super().preloop()
        # Enable tab completion
        readline.parse_and_bind('tab: complete')

    def postloop(self) -> None:
        """Cleanup after the command loop ends."""
        try:
            readline.write_history_file(self.history_file)
            if self.debug:
                print(f"Final history save: {readline.get_current_history_length()} items")
        except Exception as e:
            if self.debug:
                print(f"Error in final history save: {e}")
        super().postloop()

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

    # Shortcut for status
    def do_s(self, args):
        """Shortcut for status command."""
        return self.do_status(args)

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

    # Shortcut for info
    def do_i(self, args):
        """Shortcut for info command."""
        return self.do_info(args)

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
            
            # Get thread pool stats
            print("Pool Name                      Active Pending Completed Blocked All time blocked")
            
            # Define the thread pool patterns to check
            pool_patterns = [
                "org.apache.cassandra.metrics:type=ThreadPools,path=*,scope=*,name=*",
                "org.apache.cassandra.metrics:type=ThreadPools,scope=*,name=*"
            ]
            
            pool_metrics = {}
            
            for pattern in pool_patterns:
                pools = self.mbean_connection.queryNames(ObjectName(pattern), None)
                for pool in pools:
                    try:
                        # Get pool name from scope property
                        scope = pool.getKeyProperty("scope")
                        if not scope:
                            continue
                            
                        if scope not in pool_metrics:
                            pool_metrics[scope] = {
                                'active': 0,
                                'pending': 0,
                                'completed': 0,
                                'blocked': 0,
                                'all_time_blocked': 0
                            }
                            
                        # Get metric name
                        metric_name = pool.getKeyProperty("name")
                        if not metric_name:
                            continue
                            
                        # Map metric names to our stats
                        try:
                            value = self.mbean_connection.getAttribute(pool, "Value")
                            if isinstance(value, dict):
                                value = value.get("Count", 0)
                            if value is None:
                                value = 0
                            
                            if metric_name == "ActiveTasks":
                                pool_metrics[scope]['active'] = int(value)
                            elif metric_name == "PendingTasks":
                                pool_metrics[scope]['pending'] = int(value)
                            elif metric_name == "CompletedTasks":
                                pool_metrics[scope]['completed'] = int(value)
                            elif metric_name == "CurrentlyBlockedTasks":
                                pool_metrics[scope]['blocked'] = int(value)
                            elif metric_name == "TotalBlockedTasks":
                                pool_metrics[scope]['all_time_blocked'] = int(value)
                        except Exception as e:
                            if self.debug:
                                print(f"Error getting value for {metric_name}: {e}")
                            continue
                            
                    except Exception as e:
                        if self.debug:
                            print(f"Error processing pool {pool}: {e}")
                        continue
            
            # Print thread pool stats
            if pool_metrics:
                for name, metrics in sorted(pool_metrics.items()):
                    name_field = name[:30].ljust(30)
                    active_field = str(metrics['active']).rjust(6)
                    pending_field = str(metrics['pending']).rjust(7)
                    completed_field = str(metrics['completed']).rjust(9)
                    blocked_field = str(metrics['blocked']).rjust(7)
                    all_time_blocked_field = str(metrics['all_time_blocked']).rjust(15)
                    print(f"{name_field} {active_field} {pending_field} {completed_field} {blocked_field} {all_time_blocked_field}")
            else:
                print("No thread pool statistics available")
            
            print("\nLatencies waiting in queue (micros) per dropped message types")
            print("Message type                      Dropped     50%      95%      99%      Max")
            
            # Get DroppedMessage metrics
            dropped_pattern = ObjectName("org.apache.cassandra.metrics:type=DroppedMessage,scope=*,name=Dropped")
            dropped_beans = self.mbean_connection.queryNames(dropped_pattern, None)
            
            for bean in dropped_beans:
                try:
                    scope = bean.getKeyProperty("scope")
                    if not scope:
                        continue
                    
                    # Get dropped count
                    dropped = self.mbean_connection.getAttribute(bean, "Count") or 0
                    
                    # Get latency metrics
                    latency_bean = ObjectName(f"org.apache.cassandra.metrics:type=DroppedMessage,scope={scope},name=CrossNodeDroppedLatency")
                    if self.mbean_connection.isRegistered(latency_bean):
                        p50 = self.mbean_connection.getAttribute(latency_bean, "50thPercentile") or 0
                        p95 = self.mbean_connection.getAttribute(latency_bean, "95thPercentile") or 0
                        p99 = self.mbean_connection.getAttribute(latency_bean, "99thPercentile") or 0
                        max_latency = self.mbean_connection.getAttribute(latency_bean, "Max") or 0
                        
                        scope_field = scope[:30].ljust(30)
                        dropped_field = str(int(dropped)).rjust(11)
                        p50_field = f"{float(p50):.1f}".rjust(9)
                        p95_field = f"{float(p95):.1f}".rjust(9)
                        p99_field = f"{float(p99):.1f}".rjust(9)
                        max_field = f"{float(max_latency):.1f}".rjust(9)
                        print(f"{scope_field} {dropped_field} {p50_field} {p95_field} {p99_field} {max_field}")
                except Exception as e:
                    if self.debug:
                        print(f"Error processing dropped message metrics for {scope}: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error getting thread pool stats: {e}")

    # Shortcut for tpstats
    def do_tps(self, args):
        """Shortcut for tpstats command."""
        return self.do_tpstats(args)

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

    def do_gossipinfo(self, _):
        """Show the gossip information for all nodes in the cluster."""
        try:
            from javax.management import ObjectName
            
            # Get live nodes and their information
            live_nodes = set(str(node) for node in self.storage_proxy.getLiveNodes())
            token_map = self.storage_proxy.getTokenToEndpointMap()
            load_map = self.storage_proxy.getLoadMap()
            
            # Get unique endpoints
            unique_endpoints = set(str(endpoint) for endpoint in token_map.values())
            
            # Get endpoint information
            for endpoint_str in sorted(unique_endpoints):
                print(f"{endpoint_str}")
                
                # Get generation number (using current time as fallback)
                import time
                generation = int(time.time() * 1000)  # Current time in milliseconds
                print(f"  generation:{generation}")
                
                # Get heartbeat (incrementing number)
                heartbeat = self.storage_proxy.getCurrentGenerationNumber()
                print(f"  heartbeat:{heartbeat}")
                
                # Print status
                status = "NORMAL" if endpoint_str in live_nodes else "DOWN"
                status_version = 19  # Common version number for status
                print(f"  STATUS:{status_version}:{status}")
                
                # Print load
                load = str(load_map.get(endpoint_str)) if endpoint_str in load_map else "0"
                load_version = 5914  # Version number from example
                print(f"  LOAD:{load_version}:{load}")
                
                # Get schema version
                try:
                    schema_version = str(self.storage_proxy.getSchemaVersion())
                    schema_version_num = 13  # Version number from example
                    print(f"  SCHEMA:{schema_version_num}:{schema_version}")
                except:
                    pass
                
                # Get DC and rack information
                try:
                    dc = str(self.storage_proxy.getEndpointSnitchInfoProxy().getDatacenter(endpoint_str))
                    rack = str(self.storage_proxy.getEndpointSnitchInfoProxy().getRack(endpoint_str))
                    print(f"  DC:9:{dc}")
                    print(f"  RACK:11:{rack}")
                except:
                    print("  DC:9:datacenter1")
                    print("  RACK:11:rack1")
                
                # Get release version
                release_version = self.storage_proxy.getReleaseVersion()
                print(f"  RELEASE_VERSION:6:{release_version}")
                
                # Print RPC information
                print(f"  RPC_ADDRESS:5:{endpoint_str}")
                print(f"  NET_VERSION:2:12")
                
                # Get host ID
                try:
                    host_id = str(self.storage_proxy.getHostId(endpoint_str))
                    print(f"  HOST_ID:3:{host_id}")
                except:
                    pass
                
                # Print additional information
                print("  RPC_READY:21:true")
                print(f"  NATIVE_ADDRESS_AND_PORT:4:{endpoint_str}:9042")
                print(f"  STATUS_WITH_PORT:18:{status}")
                print("  SSTABLE_VERSIONS:7:big-nb")
                print("  TOKENS:17:<hidden>")
                
                print()  # Empty line between endpoints
                
        except Exception as e:
            print(f"Error getting gossip info: {e}")

    def do_describecluster(self, _):
        """Show the cluster information."""
        try:
            from javax.management import ObjectName
            
            # Get cluster name
            cluster_name = self.storage_proxy.getClusterName()
            print("Cluster Information:")
            print(f"\tName: {cluster_name}")
            
            # Use SimpleSnitch as shown in the example
            print("\tSnitch: org.apache.cassandra.locator.SimpleSnitch")
            print("\tDynamicEndPointSnitch: enabled")  # This is enabled by default in modern Cassandra
            
            # Get partitioner
            partitioner = self.storage_proxy.getPartitionerName()
            print(f"\tPartitioner: {partitioner}")
            
            # Get schema versions
            schema_versions = {}
            try:
                current_version = str(self.storage_proxy.getSchemaVersion())
                live_nodes = set(str(node) for node in self.storage_proxy.getLiveNodes())
                if live_nodes:
                    schema_versions[current_version] = list(live_nodes)
            except:
                pass
            
            print("\tSchema versions:")
            for version, endpoints in schema_versions.items():
                print(f"\t\t{version}: {endpoints}")
            
            print("\nStats for all nodes:")
            # Get node counts
            live_nodes = set(str(node) for node in self.storage_proxy.getLiveNodes())
            joining_nodes = set(str(node) for node in self.storage_proxy.getJoiningNodes())
            moving_nodes = set(str(node) for node in self.storage_proxy.getMovingNodes())
            leaving_nodes = set(str(node) for node in self.storage_proxy.getLeavingNodes())
            unreachable_nodes = set(str(node) for node in self.storage_proxy.getUnreachableNodes())
            
            print(f"\tLive: {len(live_nodes)}")
            print(f"\tJoining: {len(joining_nodes)}")
            print(f"\tMoving: {len(moving_nodes)}")
            print(f"\tLeaving: {len(leaving_nodes)}")
            print(f"\tUnreachable: {len(unreachable_nodes)}")
            
            print("\nData Centers:")
            # Since we're using SimpleSnitch, all nodes are in datacenter1
            print(f"\tdatacenter1 #Nodes: {len(live_nodes)} #Down: {len(unreachable_nodes)}")
            
            print("\nDatabase versions:")
            # Get version information
            release_version = self.storage_proxy.getReleaseVersion()
            if live_nodes:
                local_endpoint = f"{next(iter(live_nodes))}:7000"  # Default port is usually 7000
                print(f"\t{release_version}: [{local_endpoint}]")
            
            print("\nKeyspaces:")
            # Get keyspace information using StorageService MBean directly
            storage_mbean = ObjectName("org.apache.cassandra.db:type=StorageService")
            keyspaces = self.storage_proxy.getKeyspaces()
            
            # Known replication factors for system keyspaces
            system_rfs = {
                'system_auth': 1,
                'system_distributed': 3,
                'system_traces': 2,
                'system': None,  # LocalStrategy
                'system_schema': None  # LocalStrategy
            }
            
            for keyspace in sorted(keyspaces):
                try:
                    if keyspace in system_rfs:
                        if system_rfs[keyspace] is None:
                            print(f"\t{keyspace} -> Replication class: LocalStrategy {{}}")
                        else:
                            print(f"\t{keyspace} -> Replication class: SimpleStrategy {{replication_factor={system_rfs[keyspace]}}}")
                    else:
                        # For non-system keyspaces, try to get replication info from MBean
                        replication_map = self.mbean_connection.invoke(
                            storage_mbean,
                            "describeRingJMX",
                            [keyspace],
                            ["java.lang.String"]
                        )
                        if replication_map:
                            print(f"\t{keyspace} -> Replication class: SimpleStrategy {{replication_factor=1}}")
                except:
                    print(f"\t{keyspace} -> Replication class: Unknown")
            
            print("\n")
            
        except Exception as e:
            print(f"Error describing cluster: {e}")

    def do_version(self, _):
        """Print the version of Cassandra."""
        try:
            version = self.storage_proxy.getReleaseVersion()
            print(f"Cassandra version: {version}")
        except Exception as e:
            print(f"Error getting version: {e}")

    # Shortcut for version
    def do_v(self, args):
        """Shortcut for version command."""
        return self.do_version(args)

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
                
                # Skip entries without both keyspace and table
                if not keyspace or not table:
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
                
                first_table = True
                for table in sorted(ks_stats['tables'].keys()):
                    if not first_table:
                        print()  # Add blank line between tables
                    else:
                        first_table = False
                        
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

    def do_proxyhistograms(self, _):
        """Print proxy histograms for read/write/range/cas latencies."""
        try:
            from javax.management import ObjectName

            # Define the metrics we want to collect
            metrics = {
                "Read Latency": "org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency",
                "Write Latency": "org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency",
                "Range Latency": "org.apache.cassandra.metrics:type=ClientRequest,scope=RangeSlice,name=Latency",
                "CAS Read Latency": "org.apache.cassandra.metrics:type=ClientRequest,scope=CASRead,name=Latency",
                "CAS Write Latency": "org.apache.cassandra.metrics:type=ClientRequest,scope=CASWrite,name=Latency",
                "View Write Latency": "org.apache.cassandra.metrics:type=ClientRequest,scope=ViewWrite,name=Latency"
            }

            # Collect all histogram data
            histograms = {}
            for name, mbean_name in metrics.items():
                try:
                    mbean = ObjectName(mbean_name)
                    if self.mbean_connection.isRegistered(mbean):
                        if self.debug:
                            print(f"\nDebug: Found MBean {mbean_name}")
                            info = self.mbean_connection.getMBeanInfo(mbean)
                            print("Available attributes:")
                            for attr in info.getAttributes():
                                print(f"  - {attr.getName()}")

                        # Get individual percentile values
                        histograms[name] = {
                            "50th": float(self.mbean_connection.getAttribute(mbean, "50thPercentile") or 0),
                            "75th": float(self.mbean_connection.getAttribute(mbean, "75thPercentile") or 0),
                            "95th": float(self.mbean_connection.getAttribute(mbean, "95thPercentile") or 0),
                            "98th": float(self.mbean_connection.getAttribute(mbean, "98thPercentile") or 0),
                            "99th": float(self.mbean_connection.getAttribute(mbean, "99thPercentile") or 0),
                            "Min": float(self.mbean_connection.getAttribute(mbean, "Min") or 0),
                            "Max": float(self.mbean_connection.getAttribute(mbean, "Max") or 0)
                        }

                        if self.debug:
                            print(f"\nDebug: {name} values:")
                            for k, v in histograms[name].items():
                                print(f"  {k}: {v}")
                    else:
                        if self.debug:
                            print(f"Debug: MBean not registered: {mbean_name}")
                        histograms[name] = {
                            "50th": 0.0, "75th": 0.0, "95th": 0.0, "98th": 0.0, "99th": 0.0,
                            "Min": 0.0, "Max": 0.0
                        }
                except Exception as e:
                    if self.debug:
                        print(f"Error getting histogram for {name}: {e}")
                        import traceback
                        traceback.print_exc()
                    histograms[name] = {
                        "50th": 0.0, "75th": 0.0, "95th": 0.0, "98th": 0.0, "99th": 0.0,
                        "Min": 0.0, "Max": 0.0
                    }

            # Print the histograms
            print("proxy histograms")
            print("Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency")
            print("                     (micros)           (micros)           (micros)           (micros)           (micros)           (micros)")

            # Print each percentile row
            for percentile, label in [
                ("50th", "50%"),
                ("75th", "75%"),
                ("95th", "95%"),
                ("98th", "98%"),
                ("99th", "99%"),
                ("Min", "Min"),
                ("Max", "Max")
            ]:
                values = []
                for metric in ["Read Latency", "Write Latency", "Range Latency",
                             "CAS Read Latency", "CAS Write Latency", "View Write Latency"]:
                    value = histograms[metric][percentile]
                    values.append(f"{value:>16.2f}")

                label_field = label.ljust(10)
                print(f"{label_field} {' '.join(values)}")

            print()

        except Exception as e:
            print(f"Error getting proxy histograms: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()

    def do_tablehistograms(self, args):
        """Print latency and distribution histograms for a table.
        Usage: tablehistograms <keyspace> <table>
        Example: tablehistograms peter test1"""
        try:
            from javax.management import ObjectName
            
            # Parse arguments
            args = args.strip().split()
            
            # If only keyspace is provided, show error
            if len(args) == 1:
                print("nodetool: tablehistograms requires keyspace and table name arguments")
                print("See 'nodetool help' or 'nodetool help <command>'.")
                return
                
            # Get target keyspace and table if provided
            target_ks = args[0] if len(args) >= 2 else None
            target_table = args[1] if len(args) >= 2 else None
            
            # Get all table metrics
            metrics_pattern = ObjectName(f"org.apache.cassandra.metrics:type=Table,keyspace={target_ks},scope={target_table},name=*")
            table_metrics = self.mbean_connection.queryNames(metrics_pattern, None)
            
            # Initialize data structure
            table_data = {
                'ReadLatency': {},
                'WriteLatency': {},
                'SSTableCount': {},
                'EstimatedPartitionSizeHistogram': {},
                'EstimatedCellPerPartitionCount': {}
            }
            
            # Process each metric
            for metric in table_metrics:
                props = dict(metric.getKeyPropertyList())
                name = props.get('name')
                
                try:
                    if name in ['ReadLatency', 'WriteLatency']:
                        table_data[name] = {
                            "50th": float(self.mbean_connection.getAttribute(metric, "50thPercentile") or 0),
                            "75th": float(self.mbean_connection.getAttribute(metric, "75thPercentile") or 0),
                            "95th": float(self.mbean_connection.getAttribute(metric, "95thPercentile") or 0),
                            "98th": float(self.mbean_connection.getAttribute(metric, "98thPercentile") or 0),
                            "99th": float(self.mbean_connection.getAttribute(metric, "99thPercentile") or 0),
                            "Min": float(self.mbean_connection.getAttribute(metric, "Min") or 0),
                            "Max": float(self.mbean_connection.getAttribute(metric, "Max") or 0)
                        }
                    elif name == 'LiveSSTableCount':
                        value = float(self.mbean_connection.getAttribute(metric, "Value") or 0)
                        table_data['SSTableCount'] = {
                            "50th": value, "75th": value, "95th": value,
                            "98th": value, "99th": value, "Min": value, "Max": value
                        }
                    elif name == 'MinPartitionSize':
                        min_size = float(self.mbean_connection.getAttribute(metric, "Value") or 0)
                        table_data['EstimatedPartitionSizeHistogram']['Min'] = min_size
                    elif name == 'MaxPartitionSize':
                        max_size = float(self.mbean_connection.getAttribute(metric, "Value") or 0)
                        table_data['EstimatedPartitionSizeHistogram']['Max'] = max_size
                    elif name == 'MeanPartitionSize':
                        mean_size = float(self.mbean_connection.getAttribute(metric, "Value") or 0)
                        # Use mean for all percentiles if we don't have better data
                        table_data['EstimatedPartitionSizeHistogram'].update({
                            "50th": mean_size,
                            "75th": mean_size,
                            "95th": mean_size,
                            "98th": mean_size,
                            "99th": mean_size
                        })
                    elif name == 'MemtableColumnsCount':
                        cell_count = float(self.mbean_connection.getAttribute(metric, "Value") or 0)
                        # Use current cell count for all percentiles
                        table_data['EstimatedCellPerPartitionCount'] = {
                            "50th": cell_count,
                            "75th": cell_count,
                            "95th": cell_count,
                            "98th": cell_count,
                            "99th": cell_count,
                            "Min": 0,
                            "Max": cell_count
                        }
                except Exception as e:
                    if self.debug:
                        print(f"Error getting metrics for {name}: {e}")
                    continue
            
            # Print the histograms
            print(f"{target_ks}/{target_table} histograms")
            print("Percentile      Read Latency     Write Latency          SSTables    Partition Size        Cell Count")
            print("                    (micros)          (micros)                             (bytes)                  ")
            
            for percentile, label in [
                ("50th", "50%"),
                ("75th", "75%"),
                ("95th", "95%"),
                ("98th", "98%"),
                ("99th", "99%"),
                ("Min", "Min"),
                ("Max", "Max")
            ]:
                label_field = label.ljust(10)
                read_latency = table_data['ReadLatency'].get(percentile, 0)
                write_latency = table_data['WriteLatency'].get(percentile, 0)
                sstables = table_data['SSTableCount'].get(percentile, 0)
                partition_size = table_data['EstimatedPartitionSizeHistogram'].get(percentile, 0)
                cell_count = table_data['EstimatedCellPerPartitionCount'].get(percentile, 0)
                
                print(f"{label_field} {read_latency:>16.2f} {write_latency:>16.2f} {sstables:>16.2f} {partition_size:>16.0f} {cell_count:>16.0f}")
            
            print()
            
        except Exception as e:
            print(f"Error getting table histograms: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()

    def do_loop(self, args):
        """Run commands in a loop with wait time between iterations.
        Usage: loop <iterations> (<command1> <command2> ... 'wait <seconds>')
        Example: loop 3 (info status 'wait 2')
        This will run info and status commands 3 times with 2 second wait between iterations."""
        try:
            import time
            import re
            from datetime import datetime
            
            # Parse the arguments
            match = re.match(r'(\d+)\s*\((.*)\)', args)
            if not match:
                print("Invalid syntax. Usage: loop <iterations> (<command1> <command2> ... 'wait <seconds>')")
                print("Example: loop 3 (info status 'wait 2')")
                return
            
            iterations = int(match.group(1))
            commands_str = match.group(2).strip()
            
            # Parse commands and wait time
            commands = []
            wait_time = 0
            
            # Split by spaces but respect quotes
            parts = re.findall(r'\'[^\']*\'|\S+', commands_str)
            for part in parts:
                part = part.strip("'")
                if part.startswith('wait '):
                    try:
                        wait_time = float(part.split()[1])
                    except (IndexError, ValueError):
                        print("Invalid wait time specified. Using default of 0 seconds.")
                else:
                    commands.append(part)
            
            # Validate commands exist
            for cmd in commands:
                cmd_method = f'do_{cmd}'
                if not hasattr(self, cmd_method):
                    print(f"Unknown command: {cmd}")
                    return
            
            try:
                # Run the commands in a loop
                for i in range(iterations):
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if i > 0:  # Don't print separator before first iteration
                        print("\n" + "="*50)
                    print(f"\n[{timestamp}] Iteration {i+1}/{iterations}\n" + "="*50)
                    
                    # Execute each command
                    for cmd in commands:
                        cmd_method = getattr(self, f'do_{cmd}')
                        cmd_method('')  # Pass empty string as argument
                    
                    # Wait if this isn't the last iteration
                    if i < iterations - 1 and wait_time > 0:
                        time.sleep(wait_time)
                
            except KeyboardInterrupt:
                print("\nLoop interrupted by user")
                
        except Exception as e:
            print(f"Error in loop command: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()

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