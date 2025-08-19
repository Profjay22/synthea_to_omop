# Test basic imports and functionality
print("🧪 Testing Basic Setup...")
print("=" * 40)

try:
    # Test imports
    from config.database import DatabaseConfig
    from src.database.connection import DatabaseManager
    from src.extractors.synthea_extractor import SyntheaExtractor
    from src.utils.logging import setup_logging
    print("✅ All imports successful!")
    
    # Test config loading
    config = DatabaseConfig.from_env()
    print(f"✅ Config loaded: {config.database}")
    print(f"   - Host: {config.host}")
    print(f"   - User: {config.username}")
    print(f"   - CDM Schema: {config.schema_cdm}")
    print(f"   - Vocab Schema: {config.schema_vocab}")
    
    # Test database connection
    print("\n🔍 Testing database connection...")
    db_manager = DatabaseManager(config)
    if db_manager.test_connection():
        print("✅ Database connection successful!")
        
        # Test a simple query
        try:
            result = db_manager.execute_query("SELECT 1 as test")
            print(f"✅ Query test successful: {result['test'].iloc[0]}")
        except Exception as e:
            print(f"⚠️ Query test failed: {e}")
            
    else:
        print("❌ Database connection failed!")
    
    # Test Synthea data access
    print("\n🔍 Testing Synthea data access...")
    try:
        synthea_path = "/Users/24357405/Desktop/synthea/output/csv"
        extractor = SyntheaExtractor(synthea_path)
        summary = extractor.get_data_summary()
        
        print("✅ Synthea data access successful!")
        print("📊 Data summary:")
        for table, count in summary.items():
            if count > 0:
                print(f"   - {table}: {count:,} records")
            else:
                print(f"   - {table}: No data found")
                
    except Exception as e:
        print(f"❌ Synthea data access failed: {e}")
    
    print("\n🎉 Basic setup test completed!")
    print("👉 Next step: Run 'python verify_setup.py' for full verification")
        
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all Python files are created correctly")
except Exception as e:
    print(f"❌ Unexpected error: {e}")