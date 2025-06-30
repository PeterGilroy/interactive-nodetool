#!/usr/bin/env python3

import sys
import os

# Add the current directory to the path so we can import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from interactive_nodetool import detect_cassandra_version, CassandraVersion

# Mock storage proxy for testing
class MockStorageProxy:
    def __init__(self, release_version, git_sha=None):
        self.release_version = release_version
        self.git_sha = git_sha
    
    def getReleaseVersion(self):
        return self.release_version
    
    def getGitSHA(self):
        return self.git_sha

# Test the specific case from the user
print("Testing DSE 6.9.2 detection...")
mock_proxy = MockStorageProxy('4.0.0.692')
result = detect_cassandra_version(mock_proxy)
print(f"Input: 4.0.0.692")
print(f"Result: {result}")
print(f"Is DSE: {result.is_dse}")
print(f"Expected: DSE 6.9.2")
print(f"Match: {str(result) == 'DSE 6.9.2'}") 