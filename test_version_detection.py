#!/usr/bin/env python3

"""
Test script for version detection and compatibility logic.
This can be run without connecting to a Cassandra instance.
"""

import sys
import os

# Add the current directory to the path so we can import our classes
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import our version classes
from interactive_nodetool import CassandraVersion, VersionCompatibility, detect_cassandra_version

def test_version_detection():
    """Test the version detection logic with various version strings."""
    
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
        # DSE test cases
        {
            'name': 'DSE 6.9.2 with git SHA',
            'release_version': '4.0.0.692',
            'git_sha': 'some-commit-DSE-6.9.2-other-stuff',
            'expected': 'DSE 6.9.2'
        },
        {
            'name': 'DSE 6.9.2 without git SHA',
            'release_version': '4.0.0.692',
            'git_sha': None,
            'expected': 'DSE 6.9.2'
        },
        {
            'name': 'DSE 5.1.46',
            'release_version': '3.11.3.5146',
            'git_sha': None,
            'expected': 'DSE 5.1.46'
        },
        {
            'name': 'DSE 6.8.15',
            'release_version': '4.0.0.6815',
            'git_sha': None,
            'expected': 'DSE 6.8.15'
        },
        # Regular Cassandra test cases
        {
            'name': 'Cassandra 5.0.4',
            'release_version': '5.0.4',
            'git_sha': 'some-commit-5.0.4',
            'expected': '5.0.4'
        },
        {
            'name': 'Cassandra 4.1.0',
            'release_version': '4.1.0',
            'git_sha': None,
            'expected': '4.1.0'
        },
        {
            'name': 'Cassandra 4.0.0',
            'release_version': '4.0.0',
            'git_sha': None,
            'expected': '4.0.0'
        }
    ]
    
    print("Testing version detection logic...")
    print("=" * 50)
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print(f"  Release Version: {test_case['release_version']}")
        print(f"  Git SHA: {test_case['git_sha']}")
        print(f"  Expected: {test_case['expected']}")
        
        # Create mock storage proxy
        mock_proxy = MockStorageProxy(test_case['release_version'], test_case['git_sha'])
        
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

def test_cassandra_version_class():
    """Test the CassandraVersion class parsing."""
    
    test_cases = [
        ('DSE 6.9.2', True, 6, 9, 2),
        ('DSE 5.1.46', True, 5, 1, 46),
        ('DSE 6.8.15', True, 6, 8, 15),
        ('5.0.4', False, 5, 0, 4),
        ('4.1.0', False, 4, 1, 0),
        ('4.0.0', False, 4, 0, 0),
    ]
    
    print("\nTesting CassandraVersion class...")
    print("=" * 50)
    
    passed = 0
    failed = 0
    
    for version_string, expected_is_dse, expected_major, expected_minor, expected_patch in test_cases:
        print(f"\nTest: {version_string}")
        
        try:
            version = CassandraVersion(version_string)
            print(f"  Is DSE: {version.is_dse} (expected: {expected_is_dse})")
            print(f"  Major: {version.major} (expected: {expected_major})")
            print(f"  Minor: {version.minor} (expected: {expected_minor})")
            print(f"  Patch: {version.patch} (expected: {expected_patch})")
            
            if (version.is_dse == expected_is_dse and 
                version.major == expected_major and 
                version.minor == expected_minor and 
                version.patch == expected_patch):
                print("  ✅ PASS")
                passed += 1
            else:
                print("  ❌ FAIL")
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

def test_version_parsing():
    """Test version parsing for different formats."""
    print("Testing version parsing...")
    
    # Test Cassandra versions
    test_cases = [
        ("5.0.4", False, 5, 0, 4),
        ("4.1.0", False, 4, 1, 0),
        ("4.0.0", False, 4, 0, 0),
        ("DSE 6.8.0", True, 6, 8, 0),
        ("DSE 6.9.0", True, 6, 9, 0),
        ("DSE 5.1.0", True, 5, 1, 0),
    ]
    
    for version_str, expected_is_dse, expected_major, expected_minor, expected_patch in test_cases:
        version = CassandraVersion(version_str)
        print(f"  {version_str}:")
        print(f"    Is DSE: {version.is_dse} (expected: {expected_is_dse})")
        print(f"    Major: {version.major} (expected: {expected_major})")
        print(f"    Minor: {version.minor} (expected: {expected_minor})")
        print(f"    Patch: {version.patch} (expected: {expected_patch})")
        
        # Verify the results
        assert version.is_dse == expected_is_dse, f"Expected is_dse={expected_is_dse}, got {version.is_dse}"
        assert version.major == expected_major, f"Expected major={expected_major}, got {version.major}"
        assert version.minor == expected_minor, f"Expected minor={expected_minor}, got {version.minor}"
        assert version.patch == expected_patch, f"Expected patch={expected_patch}, got {version.patch}"
        print(f"    ✅ PASSED")

def test_version_comparisons():
    """Test version comparison methods."""
    print("\nTesting version comparisons...")
    
    version_50 = CassandraVersion("5.0.4")
    version_41 = CassandraVersion("4.1.0")
    version_40 = CassandraVersion("4.0.0")
    version_dse_68 = CassandraVersion("DSE 6.8.0")
    
    # Test is_at_least
    assert version_50.is_at_least(5, 0), "5.0.4 should be at least 5.0"
    assert version_50.is_at_least(4, 0), "5.0.4 should be at least 4.0"
    assert not version_41.is_at_least(5, 0), "4.1.0 should not be at least 5.0"
    assert version_dse_68.is_at_least(6, 8), "DSE 6.8.0 should be at least 6.8"
    
    print("  ✅ Version comparisons passed")

def test_compatibility_features():
    """Test compatibility feature detection."""
    print("\nTesting compatibility features...")
    
    test_versions = [
        ("5.0.4", "Cassandra 5.0"),
        ("4.1.0", "Cassandra 4.1"),
        ("4.0.0", "Cassandra 4.0"),
        ("DSE 6.8.0", "DSE 6.8"),
        ("DSE 5.1.0", "DSE 5.1"),
    ]
    
    for version_str, description in test_versions:
        version = CassandraVersion(version_str)
        compat = VersionCompatibility(version)
        
        print(f"  {description} ({version_str}):")
        
        # Test gossip method
        gossip_method = compat.get_gossip_info_method()
        print(f"    Gossip method: {gossip_method}")
        
        # Test application states
        states = compat.get_application_states()
        print(f"    Application states: {len(states)} available")
        
        # Test deprecated methods
        deprecated = compat.get_deprecated_methods()
        print(f"    Deprecated methods: {len(deprecated)}")
        
        # Test features
        features = compat.get_version_info()['features']
        print(f"    SSL debug: {features['ssl_debug']}")
        print(f"    Audit logging: {features['audit_logging']}")
        print(f"    Full query logging: {features['full_query_logging']}")
        print(f"    CIDR filtering: {features['cidr_filtering']}")
        
        print(f"    ✅ PASSED")

def test_dse_detection():
    """Test DSE-specific detection."""
    print("\nTesting DSE detection...")
    
    dse_versions = [
        "DSE 6.8.0",
        "DSE 6.9.0", 
        "DSE 5.1.0"
    ]
    
    for version_str in dse_versions:
        version = CassandraVersion(version_str)
        compat = VersionCompatibility(version)
        
        print(f"  {version_str}:")
        print(f"    Is DSE: {version.is_dse}")
        print(f"    Metrics patterns: {list(compat.get_metrics_patterns().keys())}")
        
        # Check for DSE-specific patterns
        patterns = compat.get_metrics_patterns()
        assert 'dse_metrics' in patterns, f"DSE metrics should be available for {version_str}"
        assert 'search_metrics' in patterns, f"Search metrics should be available for {version_str}"
        
        print(f"    ✅ PASSED")

if __name__ == "__main__":
    print("Running version detection tests...")
    print("=" * 50)
    
    try:
        test1_passed = test_version_detection()
        test2_passed = test_cassandra_version_class()
        
        if test1_passed and test2_passed:
            print("\n🎉 All tests passed!")
            sys.exit(0)
        else:
            print("\n❌ Some tests failed!")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 