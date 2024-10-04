import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist

class LogicManager:
    def __init__(self) -> None:
        pass

    def get_best_fit_function(self, xy_train_func:pd.DataFrame, xy_all_ideal_func:pd.DataFrame):
        '''
        Find the best fitting ideal function for a train function

        :param xy_train_func: a train function
        :param xy_all_ideal_func: all possible ideal functions
        :return: index of best fitting function
        '''
        # Ensure x values match
        if not np.array_equal(xy_train_func.iloc[:, 0], xy_all_ideal_func.iloc[:, 0]):
            raise ValueError("X values in training and ideal datasets do not match")

        y_train = xy_train_func.iloc[:, 1].values
        best_function  = -1
        smalest_deviation = float('inf')
        for column in range(1, 51):
            y_ideal = xy_all_ideal_func.iloc[:, column].values
            # Least squares calculation
            deviation = np.sum((y_train - y_ideal) ** 2)  

            # Update with better function if smaller diviations has been found
            if deviation < smalest_deviation:
                smalest_deviation = deviation
                best_function = column

        return best_function

    def calculate_max_deviation(self, xy_train: np.array, xy_ideal: np.array) -> float:
        """
        Calculate the maximum point-wise Euclidean deviation between training data and ideal function.

        :param xy_train: Array of (x, y) coordinates of the training data
        :param xy_ideal: Array of (x, y) coordinates of the ideal function
        :return: Maximum deviation
        """
        # Ensure the input arrays are 2D
        xy_train = np.atleast_2d(xy_train)
        xy_ideal = np.atleast_2d(xy_ideal)

        # Calculate pairwise distances between all points
        distances = cdist(xy_train, xy_ideal)

        # For each training point, find the minimum distance to any ideal point
        min_distances = np.min(distances, axis=1)

        # Return the maximum of these minimum distances
        return np.max(min_distances)

    def validate_deviation(self, x_value, y_value, xy_func: pd.DataFrame, max_deviation):
        """
        Validate if the (x,y) coordiate fit into the max_diviation of the xy_func

        :param x_value: x value of coordinate
        :param y_value: y value of coordinate
        :param xy_func: function to validate with
        :param max_deviation: maximum deviation to function
        :return: If validatet the deviation of the coordinate, otherwise None
        """
        # Find the closest point on the curve
        distances = np.sqrt((xy_func.iloc[:, 0] - x_value)**2 + (xy_func.iloc[:, 1] - y_value)**2)
        closest_index = distances.idxmin()

        x_value_func = xy_func.iloc[closest_index, 0]
        y_value_func = xy_func.iloc[closest_index, 1]

        # Calculate the Euclidean distance
        deviation = np.sqrt((x_value - x_value_func)**2 + (y_value - y_value_func)**2)

        # Check if deviation doesn't exceed max_deviation
        if deviation <= max_deviation :
            return deviation

        # Coordinate could not be validated
        return None


    def find_best_function_test(self, x_value, y_value, dataFrame_ideal:pd.DataFrame, pd_func_max_div:pd.DataFrame):
        """
        Validate if the (x,y) coordiate fit into the max_diviation of the xy_func

        :param x_value: x value of coordinate
        :param y_value: y value of coordinate
        :param dataFrame_ideal: all ideal function
        :param pd_func_max_div: array with (choosen function, max deviation)
        :return: returns best deviation and the best fitting function
        """
        best_deviation = None
        best_function = None

        # Loop over all functions
        for index, row in pd_func_max_div.iterrows():
            func_id = row['func_id']
            max_div = row['max_div']

            deviation = self.validate_deviation(x_value, y_value, dataFrame_ideal.iloc[:, [0, func_id]], max_div)

            # Check if the result is the better option
            if deviation != None:
                if best_deviation == None or deviation < best_deviation:
                    best_deviation = deviation
                    best_function = func_id

        # Return solution
        return best_deviation, best_function

