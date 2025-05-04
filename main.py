import src.sql_manager as sql_mgr
import src.logic_manager as lgc_mgr
import src.visual_manager as v_mgr
import src.unit_test_manager as ut_mgr
import os
import pandas as pd
import numpy as np

# Enable or disable unit testing.
unit_testing = True
# Define the location of the csv data.
csv_location = os.path.abspath("./data/")

def main():

    # -----------------------------UNIT TESTS------------------------------- #
    if unit_testing:
        # Create the UnitTestManager.
        ut_manager = ut_mgr.UnitTestManager()
        # Start the testing procedure.
        ut_manager.perform_unit_tests()
        ut_manager.print_results()

        if ut_manager.status == "FAILED":
            print("\nAt least one unit tests failed, "
                  "the program has been terminated.")
            return
        
    # -------------------------------DATABSE-------------------------------- #

    # Create the database.
    db_manager = sql_mgr.DatabaseManager("dataBase.db")
    db_manager.createDatabase()

    # Import ideal CSV into the database.
    db_manager.import_idealCSV(os.path.join(csv_location, "ideal.csv"))

    # Import train CSV into the database.
    db_manager.import_trainCSV(os.path.join(csv_location, "train.csv"))

    # Load (just created) train and ideal table.
    dataFrame_ideal = db_manager.load_table("ideal_db")
    dataFrame_train = db_manager.load_table("train_db")

    # Load test CSV into a pandas data frame.
    csv_test = db_manager.csv_2DArray(os.path.join(csv_location, "test.csv"))


    # ------------------------------LOGIC----------------------------------- #
    # Create the logicManager.
    lgc_manager = lgc_mgr.LogicManager()

    # Find the four best fitting functions.
    # Function 1
    ideal_for_y1 = lgc_manager.get_best_fit_function(
        dataFrame_train.iloc[:,[0,1]], 
        dataFrame_ideal)
    max_deviation_y1 = lgc_manager.calculate_max_deviation(
        dataFrame_train.iloc[:, [0,1]], 
        dataFrame_ideal.iloc[:, [0,ideal_for_y1]])

    # Function 2
    ideal_for_y2 = lgc_manager.get_best_fit_function(
        dataFrame_train.iloc[:,[0,2]], 
        dataFrame_ideal)
    max_deviation_y2 = lgc_manager.calculate_max_deviation(
        dataFrame_train.iloc[:, [0,2]], 
        dataFrame_ideal.iloc[:, [0, ideal_for_y2]])

    # Function 3
    ideal_for_y3 = lgc_manager.get_best_fit_function(
        dataFrame_train.iloc[:,[0,3]], 
        dataFrame_ideal)
    max_deviation_y3 = lgc_manager.calculate_max_deviation(
        dataFrame_train.iloc[:, [0,3]], 
        dataFrame_ideal.iloc[:, [0, ideal_for_y3]])

    # Function 4
    ideal_for_y4 = lgc_manager.get_best_fit_function(
        dataFrame_train.iloc[:,[0,4]], 
        dataFrame_ideal)
    max_deviation_y4 = lgc_manager.calculate_max_deviation(
        dataFrame_train.iloc[:, [0,4]], 
        dataFrame_ideal.iloc[:, [0, ideal_for_y4]])

    # Convert to usabel pandas data frame.
    # "* np.sqrt(2)" for the max diviation of the test data.
    pd_func_max_div = pd.DataFrame([
        [ideal_for_y1, max_deviation_y1 * np.sqrt(2)],
        [ideal_for_y2, max_deviation_y2 * np.sqrt(2)], 
        [ideal_for_y3, max_deviation_y3 * np.sqrt(2)],
        [ideal_for_y4, max_deviation_y4 * np.sqrt(2)]],
        columns=['func_id', 'max_div'])
    
    # Sort DataFrame by function id for later legend ordering.
    pd_func_max_div = pd_func_max_div.sort_values(by='func_id')

    # Resetting index after sorting.
    pd_func_max_div = pd_func_max_div.reset_index(drop=True)

    for index, row in csv_test.iterrows():
        x_value = csv_test.iloc[index, 0] 
        y_value = csv_test.iloc[index, 1] 

        # Find the beste function and its deviation for the test table.
        deviation_and_funcID = lgc_manager.find_best_function_test(
            x_value, y_value, dataFrame_ideal, pd_func_max_div)

        # Import result into the database.
        db_manager.testDB_add_record(
            x_value, y_value, deviation_and_funcID[0], deviation_and_funcID[1]);        


    # ---------------------------VISUALISATION------------------------------- #
    # Load the test database for the visualisation.
    dataFrame_test = db_manager.load_table("test_db")

    # Colors for each of the four functions.
    func_colors = ['#f56fa1', 
                       '#f0cf37',
                       '#90d2d8',
                       '#63bc46']

    # Create the VisualManager.
    v_manager = v_mgr.VisualManager(
        dataFrame_train, dataFrame_ideal, dataFrame_test)
    # Start visualisation procedure.
    v_manager.visualize_data_and_deviations(pd_func_max_div , func_colors)

if __name__ == "__main__":
    # Setting up that this file will be started first with its main
    main()