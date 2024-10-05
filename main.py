import src.sql_manager as sql_mgr
import src.logic_manager as lgc_mgr
import src.visual_manager as v_mgr
import pandas as pd
import numpy as np

def main():
    # -----------------------------------DATABSE----------------------------------- #

    # Create Database
    db_manager = sql_mgr.DatabaseManager("dataBase.db")
    db_manager.createDatabase()

    # Import ideal CSV into database
    db_manager.import_idealCSV("./data/ideal.csv")

    # Import train CSV into database
    db_manager.import_trainCSV("./data/train.csv")

    # Load (just created) train and ideal table
    dataFrame_ideal = db_manager.load_table("ideal_db")
    dataFrame_train = db_manager.load_table("train_db")

    # Load test CSV into pandas data frame
    csv_test = db_manager.csv_2DArray("./data/test.csv")


    # -----------------------------------LOGIC----------------------------------- #
    # Create logic manager
    lgc_manager = lgc_mgr.LogicManager()

    # Find the four best fitting functions
    # Function 1
    ideal_for_y1 = lgc_manager.get_best_fit_function(dataFrame_train.iloc[:,[0,1]], dataFrame_ideal)
    max_diviation_y1 = lgc_manager.calculate_max_deviation(dataFrame_train.iloc[:, [0,1]], dataFrame_ideal.iloc[:, [0,ideal_for_y1]])

    # Function 2
    ideal_for_y2 = lgc_manager.get_best_fit_function(dataFrame_train.iloc[:,[0,2]], dataFrame_ideal)
    max_diviation_y2 = lgc_manager.calculate_max_deviation(dataFrame_train.iloc[:, [0,2]], dataFrame_ideal.iloc[:, [0, ideal_for_y2]])

    # Function 3
    ideal_for_y3 = lgc_manager.get_best_fit_function(dataFrame_train.iloc[:,[0,3]], dataFrame_ideal)
    max_diviation_y3 = lgc_manager.calculate_max_deviation(dataFrame_train.iloc[:, [0,3]], dataFrame_ideal.iloc[:, [0, ideal_for_y3]])

    # Function 4
    ideal_for_y4 = lgc_manager.get_best_fit_function(dataFrame_train.iloc[:,[0,4]], dataFrame_ideal)
    max_diviation_y4 = lgc_manager.calculate_max_deviation(dataFrame_train.iloc[:, [0,4]], dataFrame_ideal.iloc[:, [0, ideal_for_y4]])

    # Convert to usabel pandas data frame
    pd_func_max_div = pd.DataFrame([[ideal_for_y1, max_diviation_y1 * np.sqrt(2)],  # "* np.sqrt(2)" for the max diviation of the test data
                                    [ideal_for_y2, max_diviation_y2 * np.sqrt(2)],  # -- 
                                    [ideal_for_y3, max_diviation_y3 * np.sqrt(2)],  # -- 
                                    [ideal_for_y4, max_diviation_y4 * np.sqrt(2)]], # -- 
                                    columns=['func_id', 'max_div'])

    for index, row in csv_test.iterrows():
        x_value = csv_test.iloc[index, 0] 
        y_value = csv_test.iloc[index, 1] 

        # Find the beste function and its deviation for the test table
        deviation_and_funcID = lgc_manager.find_best_function_test(x_value, y_value, dataFrame_ideal, pd_func_max_div)

        # Import result into the database  
        db_manager.testDB_add_record(x_value, y_value, deviation_and_funcID[0], deviation_and_funcID[1]);        


    # -----------------------------------VISUALISATION----------------------------------- #
    # Load the test database for visualisation
    dataFrame_test = db_manager.load_table("test_db")

    # Colors for each of the four functions
    function_colors = ['#f56fa1', 
                       '#f0de89',
                       '#90d2d8',
                       '#63bc46']

    # Create VisualManager
    v_manager = v_mgr.VisualManger(dataFrame_train, dataFrame_ideal, dataFrame_test)
    # Start visualisation procedure
    v_manager.visualize_data_and_deviations(pd_func_max_div , function_colors)

if __name__ == "__main__":
    # Setting up that this file will be started first with its main
    main()