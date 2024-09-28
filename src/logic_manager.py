import pandas as pd
import numpy as np


class LogicManager:
    def __init__(self) -> None:
        pass

    def get_best_fit_function(self, xy_train_func:pd.DataFrame, xy_all_ideal_func:pd.DataFrame):
        # Ensure x values match
        if not np.array_equal(xy_train_func.iloc[:, 0], xy_all_ideal_func.iloc[:, 0]):
            raise ValueError("X values in training and ideal datasets do not match")

        y_train = xy_train_func.iloc[:, 1].values
        best_function  = -1
        smalest_deviation = float('inf')
        for column in range(1, 51):
            y_ideal = xy_all_ideal_func.iloc[:, column].values
            deviation = np.sum((y_train - y_ideal) ** 2)  # Least squares calculation

            if deviation < smalest_deviation:
                smalest_deviation = deviation
                best_function = column

        return best_function

    def calculate_max_deviation(self, y_train: np.array, y_ideal: np.array) -> float:
        """
        Calculate the maximum point-wise deviation between training data and ideal function.

        :param y_train: Y values of the training data
        :param y_ideal: Y values of the ideal function
        :return: Maximum deviation
        """
        return np.max(np.abs(y_train - y_ideal))

    def validate_y_deviation(self, x_value, y_value, xy_func:pd.DataFrame, max_deviation):
        index_of_x = xy_func.index[xy_func.iloc[:,0] == x_value]
        if not index_of_x.empty:
            y_value_func = xy_func.iloc[index_of_x[0], 1]
            deviation = np.abs(y_value - y_value_func)
            if max_deviation > deviation + deviation**2 : # not exceed by MORE than factor sqrt(2)
                return deviation

        else:
            raise ValueError("X values isn't included in the given array")

        return None


    def find_best_function_test(self, x_value, y_value, dataFrame_ideal:pd.DataFrame, pd_func_max_div:pd.DataFrame):
        best_deviation = None
        best_function = None

        for index, row in pd_func_max_div.iterrows():
            func_id = row['func_id']
            max_div = row['max_div']

            deviation = self.validate_y_deviation(x_value, y_value, dataFrame_ideal.iloc[:, [0, func_id]], max_div)

            if deviation != None:
                if best_deviation == None or deviation < best_deviation:
                    best_deviation = deviation
                    best_function = func_id


        return best_deviation, best_function

