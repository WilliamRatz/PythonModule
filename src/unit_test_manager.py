import os
import numpy as np
import pandas as pd
import src.logic_manager as lgc_mgr
import src.sql_manager as sql_mgr

class UnitTestManager:

    def __init__(self):
        """Initialize the result array and the database path 
        of the UnitTestManager class"""
        self.test_results = []
        self.unit_test_db_path = "unit_test.db"
        
    def _cleanup_database(self):
        """Remove any existing test database files"""
        try:
            if os.path.exists(self.unit_test_db_path):
                os.remove(self.unit_test_db_path)
            # Also clean up any lingering SQLite journal files
            if os.path.exists(self.unit_test_db_path + "-journal"):
                os.remove(self.unit_test_db_path + "-journal")
        except Exception as e:
            print(f"Warning: Could not clean up test database: {str(e)}")
        
    def perform_unit_tests(self):
        """Execute all unit tests and return results"""
        # Perform all unit tests
        self._test_get_best_fit_function()
        self._test_calculate_max_deviation()
        self._test_validate_deviation()
        self._test_database_operations()

        # Format results: [(test_name, passed), ...]
        return [(name, passed) for name, passed in self.test_results]
    
    def _record_test(self, test_name, condition):
        """Helper to store test results"""
        self.test_results.append((test_name, bool(condition)))
        return condition

    def _test_get_best_fit_function(self):
        """Test ideal function selection logic"""
        lm = lgc_mgr.LogicManager()
        
        # Valid case test
        train_data = pd.DataFrame({'X': [-20, 0, 20], 'Y': [-20, 0, 20]})
        ideal_data = pd.DataFrame({
            'X': [-20, 0, 20],
            'Y1': [-19, 1, 21],  # Sum sq. diff = 1+1+1 = 3
            'Y2': [-20, 0, 20],  # Perfect fit (0)
            'Y3': [-22, 0, 22]   # Sum sq. diff = 4+0+4 = 8
        })
        
        best_fit = lm.get_best_fit_function(train_data, ideal_data)
        self._record_test("Best Fit Selection", best_fit == 2)
        
        # Test edge cases
        test_cases = [
            ("Empty Train Data", pd.DataFrame(), ideal_data),
            ("Empty Ideal Data", train_data, pd.DataFrame()),
            ("Missing Columns", pd.DataFrame({'X': [1]}), ideal_data)
        ]
        
        for name, train, ideal in test_cases:
            try:
                lm.get_best_fit_function(train, ideal)
                self._record_test(f"Error Handling: {name}", False)
            except (ValueError, IndexError):
                self._record_test(f"Error Handling: {name}", True)

    def _test_calculate_max_deviation(self):
        """Test maximum deviation calculation"""
        lgc_manager = lgc_mgr.LogicManager()
        
        # Perfect match case
        train = pd.DataFrame({'X': [1, 2], 'Y': [1, 2]})
        ideal = pd.DataFrame({'X': [1, 2], 'Y1': [1, 2]})
        max_dev = lgc_manager.calculate_max_deviation(train, ideal)
        self._record_test("Zero Deviation Case", max_dev == 0)
        
        # Normal case
        train = pd.DataFrame({'X': [1, 2], 'Y': [1, 4]})
        ideal = pd.DataFrame({'X': [1, 2], 'Y1': [1, 2]})
        max_dev = lgc_manager.calculate_max_deviation(train, ideal)
        self._record_test("Max Deviation Calculation", max_dev == 2)

    def _test_validate_deviation(self):
        """Test point validation logic"""
        lgc_manager = lgc_mgr.LogicManager()
        func_data = pd.DataFrame({'X': [-20, 0, 20], 'Y1': [-20, 0, 20]})
        
        # Valid point (on curve)
        valid = lgc_manager.validate_deviation(0, 0, func_data, 1.0)
        self._record_test("Exact Match Validation", valid == 0)
        
        # Edge case (boundary of tolerance)
        valid = lgc_manager.validate_deviation(0, 0.99, func_data, 1.0)
        self._record_test("Boundary Tolerance", valid == 0.99)
        
        # Invalid point
        valid = lgc_manager.validate_deviation(0, 1.01, func_data, 1.0)
        self._record_test("Excess Deviation", valid is None)

    def _test_database_operations(self):
        """Test database CRUD operations"""
        db_manager = sql_mgr.DatabaseManager(self.unit_test_db_path)
        success = db_manager.createDatabase()
        self._record_test("Database Creation", success)
        
        # Train record insertion
        success = db_manager.trainDB_add_record(1.0, 1.1, 1.2, 1.3, 1.4)
        self._record_test("Train Data Insertion", success)
        
        # Train duplicate handling
        success = db_manager.trainDB_add_record(1.0, 1.5, 1.6, 1.7, 1.8)
        self._record_test("Train Data Duplicate Prevention", not success)

        # Ideal record insertion
        success = db_manager.idealDB_add_record(1.0, np.random.randint(-20, 21, 50))
        self._record_test("Ideal Data Insertion", success)
        
        # Ideal duplicate handling
        success = db_manager.idealDB_add_record(1.0, np.random.randint(-20, 21, 50))
        self._record_test("Ideal Data Duplicate Prevention", not success)

        # Test record insertion
        success = db_manager.testDB_add_record(1.0, 1.1, 0.1, 100)
        self._record_test("Test Data Insertion", success)
        
        # Test duplicate handling
        success = db_manager.testDB_add_record(1.0, 1.1, 0.2, 110)
        self._record_test("Test Data Duplicate Prevention", not success)

        # Clear all exisiting connections
        db_manager.db_engine.dispose()
        
        # Delete existing unit test database file
        self._cleanup_database()