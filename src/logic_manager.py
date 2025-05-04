import pandas as pd
import numpy as np
from typing import Tuple, Optional
from src.base_manager import BaseManager 

class LogicManager(BaseManager):

    def __init__(self) -> None:
        """
        Initialization of the LogicManager.
        """
        # Initialize the parent class.
        super().__init__()

        # Change the status to "INITIALIZED".
        self.set_status("INITIALIZED")

    def get_best_fit_function(self, xy_train_func:pd.DataFrame, 
                              xy_all_ideal_func:pd.DataFrame) -> int:
        '''
        Find the best fitting ideal function for a train function.

        :param xy_train_func: a train function
        :param xy_all_ideal_func: all possible ideal functions
        :return: index of best fitting function
        '''
        # Check for empty data.
        if xy_train_func.empty or xy_all_ideal_func.empty:
            error_msg = "Input DataFrames cannot be empty."
            # Change the status to "ERROR".
            self.set_status("ERROR")
            # Log the error message.
            self.log(f"Validation failed: {error_msg}")
            raise ValueError(error_msg)
        
        # Verify required columns exist.
        if len(xy_train_func.columns) < 2 or len(xy_all_ideal_func.columns) < 2:
            error_msg = "DataFrames must contain at least X and Y columns."
            # Change the status to "ERROR".
            self.set_status("ERROR")
            # Log the error message.
            self.log(f"Validation failed: {error_msg}")
            raise ValueError(error_msg)
        
        
        # Ensure x values match.
        if not np.array_equal(xy_train_func.iloc[:, 0].values, 
                            xy_all_ideal_func.iloc[:, 0].values):
            error_msg = "X values in training and ideal datasets do not match."
            # Change the status to "ERROR".
            self.set_status("ERROR")
            # Log the error message.
            self.log(f"Validation failed: {error_msg}")
            raise ValueError(error_msg)

        y_train = xy_train_func.iloc[:, 1].values
        best_function = -1
        smallest_deviation = float('inf')
        
        # Check each Y column (skip X column at index 0).
        for column in range(1, len(xy_all_ideal_func.columns)):
            y_ideal = xy_all_ideal_func.iloc[:, column].values
            deviation = np.sum((y_train - y_ideal) ** 2)

            if deviation < smallest_deviation:
                smallest_deviation = deviation
                best_function = column

        return best_function

    def calculate_max_deviation(self, xy_train: pd.DataFrame, xy_ideal: pd.DataFrame) -> float:
        """
        Calculate the maximum Y deviation between training data and ideal 
        function at matching X values.

        :param xy_train: DataFrame with (x, y) coordinates of the training data
        :param xy_ideal: DataFrame with (x, y) coordinates of the ideal function
        :return: Maximum absolute Y deviation
        """
        # Ensure the x-values match.
        if not np.allclose(xy_train.iloc[:, 0], xy_ideal.iloc[:, 0], atol=1e-6):
            error_msg = "X values in training and ideal datasets do not match"
            # Change the status to "ERROR".
            self.set_status("ERROR")
            # Log the error message.
            self.log(f"Validation failed: {error_msg}")
            raise ValueError(error_msg)

        # Compute the absolute differences in y-values.
        y_diff = np.abs(xy_train.iloc[:, 1] - xy_ideal.iloc[:, 1])

        # Return the maximum deviation.
        return y_diff.max()

    def validate_deviation(self, x_value, y_value, 
                           xy_func: pd.DataFrame, max_deviation) -> Optional[float]:
        """
        Validate if the (x,y) coordinate fits within the maximum deviation 
        from the ideal function at the given x position.

        :param x_value: x value of coordinate
        :param y_value: y value of coordinate
        :param xy_func: ideal function to validate against
        :param max_deviation: maximum allowed deviation
        :return: deviation if validated, otherwise None
        """
        # Try to find the exact matching x_value in the ideal function.
        match = xy_func.loc[np.isclose(xy_func.iloc[:, 0], x_value, atol=1e-6)]

        if match.empty:
            # No matching x found (should normally not happen if x-values match).
            return None

        # Get corresponding y from the ideal function.
        y_value_func = match.iloc[0, 1]

        # Calculate vertical deviation (only y-axis, since x is matched).
        deviation = np.abs(y_value - y_value_func)

        if deviation <= max_deviation:
            return deviation

        # Deviation too large.
        return None

    def find_best_function_test(self, x_value, y_value, 
                                dataFrame_ideal:pd.DataFrame, 
                                pd_func_max_div:pd.DataFrame
                                ) -> Tuple[Optional[float], Optional[int]]:
        """
        Validate if the (x,y) coordiate fit into the max_deviation 
        of the xy_func.

        :param x_value: x value of coordinate
        :param y_value: y value of coordinate
        :param dataFrame_ideal: all ideal function
        :param pd_func_max_div: array with (choosen function, max deviation)
        :return: returns best deviation and the best fitting function
        """

        best_deviation = None
        best_function = None

        # Loop over all functions.
        for index, row in pd_func_max_div.iterrows():
            func_id = row['func_id']
            max_div = row['max_div']

            deviation = self.validate_deviation(x_value, y_value, 
                        dataFrame_ideal.iloc[:, [0, func_id]], max_div)

            # Check if the result is the better option.
            if deviation != None:
                if best_deviation == None or deviation < best_deviation:
                    best_deviation = deviation
                    best_function = func_id

        # Return the solution.
        return best_deviation, best_function