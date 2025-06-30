#!/usr/bin/env python3

import sys
import os

# Add the current directory to the path so we can import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from interactive_nodetool import detect_cassandra_version, CassandraVersion

def test_dse_version_detection():
    """Test DSE version detection with the actual patterns."""
    
    # Mock storage proxy for testing
    class MockStorageProxy:
        def __init__(self, release_version, git_sha=None):
            self.release_version = release_version
            self.git_sha = git_sha
        
        def getReleaseVersion(self):
            return self.release_version
        
        def getGitSHA(self):
            return self.git_sha
    
    test_cases = [
        # DSE 6.9.x test cases
        {
            'name': 'DSE 6.9.2',
            'release_version': '4.0.0.692',
            'expected': 'DSE 6.9.2'
        },
        {
            'name': 'DSE 6.9.0',
            'release_version': '4.0.0.690',
            'expected': 'DSE 6.9.0'
        },
        {
            'name': 'DSE 6.9.9',
            'release_version': '4.0.0.699',
            'expected': 'DSE 6.9.9'
        },
        # DSE 6.8.x test cases
        {
            'name': 'DSE 6.8.15',
            'release_version': '4.0.0.6815',
            'expected': 'DSE 6.8.15'
        },
        {
            'name': 'DSE 6.8.0',
            'release_version': '4.0.0.680',
            'expected': 'DSE 6.8.0'
        },
        # DSE 5.1.x test cases
        {
            'name': 'DSE 5.1.46',
            'release_version': '3.11.3.5146',
            'expected': 'DSE 5.1.46'
        },
        {
            'name': 'DSE 5.1.0',
            'release_version': '3.11.3.5100',
            'expected': 'DSE 5.1.0'
        },
        # Regular Cassandra test cases
        {
            'name': 'Cassandra 5.0.4',
            'release_version': '5.0.4',
            'expected': '5.0.4'
        },
        {
            'name': 'Cassandra 4.1.0',
            'release_version': '4.1.0',
            'expected': '4.1.0'
        }
    ]
    
    print("Testing DSE version detection...")
    print("=" * 50)
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print(f"  Release Version: {test_case['release_version']}")
        print(f"  Expected: {test_case['expected']}")
        
        # Create mock storage proxy
        mock_proxy = MockStorageProxy(test_case['release_version'])
        
        try:
            # Test the detection
            detected_version = detect_cassandra_version(mock_proxy)
            print(f"  Detected: {detected_version}")
            
            # Check if the result matches expected
            if str(detected_version) == test_case['expected']:
                print("  ✅ PASS")
                passed += 1
            else:
                print(f"  ❌ FAIL - Expected {test_case['expected']}, got {detected_version}")
                failed += 1
                
        except Exception as e:
            print(f"  ❌ ERROR - {e}")
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed!")
        return False

if __name__ == '__main__':
    success = test_dse_version_detection()
    sys.exit(0 if success else 1) 