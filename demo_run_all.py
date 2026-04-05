"""
Demo Orchestration Script - Runs all demo components in sequence
Verifies complete system functionality for video demonstration
"""
import sys
import os
import time
import subprocess
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def print_banner(text):
    """Print banner"""
    width = 80
    print("\n" + "=" * width)
    print(text.center(width))
    print("=" * width + "\n")


def run_command(script_name, description):
    """Run a demo script and report results"""
    
    print(f"\n{'='*80}")
    print(f"RUNNING: {description}")
    print(f"Script:  {script_name}")
    print(f"Time:    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*80}\n")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=False,
            timeout=120
        )
        
        success = result.returncode == 0
        
        if success:
            print(f"\n✓ {description}: SUCCESS")
        else:
            print(f"\n✗ {description}: FAILED (exit code: {result.returncode})")
        
        return success
        
    except subprocess.TimeoutExpired:
        print(f"\n✗ {description}: TIMEOUT (exceeded 120 seconds)")
        return False
    except Exception as e:
        print(f"\n✗ {description}: ERROR - {e}")
        return False


def verify_environment():
    """Verify system environment and dependencies"""
    
    print_banner("ENVIRONMENT VERIFICATION")
    
    checks = {
        'Python': sys.version.split()[0],
        'OS': sys.platform,
        'Working Directory': os.getcwd(),
    }
    
    for key, value in checks.items():
        print(f"✓ {key:<25} {value}")
    
    # Check dependencies
    print(f"\nDependency Check:")
    
    required_modules = [
        'sqlite3',
        'pymongo',
        'fastapi',
        'uvicorn'
    ]
    
    all_found = True
    for module in required_modules:
        try:
            __import__(module)
            print(f"  ✓ {module:<20} Available")
        except ImportError:
            print(f"  ✗ {module:<20} Missing")
            all_found = False
    
    print()
    return all_found


def generate_summary_report(results):
    """Generate execution summary report"""
    
    print_banner("DEMONSTRATION SUMMARY REPORT")
    
    print(f"Execution Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"\nTest Results:")
    print(f"{'Test Name':<40} {'Status':<10} {'Result'}")
    print(f"{'-'*60}")
    
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:<40} {status:<10}")
    
    total = len(results)
    passed = sum(1 for _, p in results if p)
    
    print(f"\n{'='*60}")
    print(f"Total Tests:        {total}")
    print(f"Passed:             {passed}")
    print(f"Failed:             {total - passed}")
    print(f"Pass Rate:          {100 * passed // total}%")
    print(f"{'='*60}\n")
    
    return passed == total


def main():
    """Main orchestration function"""
    
    print_banner("HYBRID DATABASE SYSTEM - COMPLETE DEMO VERIFICATION")
    
    print(f"This script will verify all components of the hybrid database system")
    print(f"used in the video demonstration.\n")
    
    # Step 1: Environment verification
    print_banner("STEP 1: ENVIRONMENT VERIFICATION")
    env_ok = verify_environment()
    
    if not env_ok:
        print("\n✗ Environment check failed. Please install missing dependencies.")
        return False
    
    print("✓ Environment check passed\n")
    
    # Step 2: Run demo components
    print_banner("STEP 2: RUNNING DEMO COMPONENTS")
    
    test_start = time.time()
    
    results = []
    
    # Demo 1: Data Setup
    print("\n[1/4] Setting up demo data...")
    results.append((
        "Data Setup & Field Placement",
        run_command('demo_setup.py', 'Data Ingestion & Placement Heuristics')
    ))
    
    time.sleep(1)
    
    # Demo 2: 2PC Transactions
    print("\n[2/4] Running 2PC transaction demo...")
    results.append((
        "Two-Phase Commit Protocol",
        run_command('demo_2pc_transactions.py', 'Transaction Coordination (2PC)')
    ))
    
    time.sleep(1)
    
    # Demo 3: ACID Tests
    print("\n[3/4] Running ACID test suite...")
    results.append((
        "ACID Properties Validation",
        run_command('demo_acid_tests.py', 'ACID Validation (A/C/I/D)')
    ))
    
    time.sleep(1)
    
    # Demo 4: Dashboard Verification
    print("\n[4/4] Verifying dashboard functionality...")
    results.append((
        "Dashboard Component Verification",
        run_command('demo_dashboard_verify.py', 'Dashboard Verification')
    ))
    
    test_end = time.time()
    test_duration = test_end - test_start
    
    # Print summary report
    print_banner(f"VERIFICATION COMPLETE (Duration: {test_duration:.1f}s)")
    
    all_passed = generate_summary_report(results)
    
    # Final verdict
    if all_passed:
        print_banner("✓ ALL COMPONENTS VERIFIED - SYSTEM READY FOR VIDEO DEMO")
        print("\nThe system is fully functional and ready for video demonstration.")
        print("All components are working correctly:")
        print("  • Data ingestion and field placement ✓")
        print("  • Transaction coordination (2PC) ✓")
        print("  • ACID property validation ✓")
        print("  • Dashboard integration ✓")
        print("\nYou can now proceed with the video recording.\n")
        return True
    else:
        print_banner("✗ SOME COMPONENTS FAILED - SYSTEM REQUIRES ATTENTION")
        print("\nPlease review the error messages above and fix the issues.")
        print("Common troubleshooting:")
        print("  • Ensure MongoDB is running: mongod")
        print("  • Check FastAPI is available: pip install fastapi")
        print("  • Verify SQLite is accessible")
        print("  • Check file permissions in working directory\n")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
