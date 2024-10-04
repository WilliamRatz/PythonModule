import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

class VisualManger:

    def __init__(self, df_train, df_ideal, df_test):
        """
        Saves localy need data fields

        :param df_train: train data
        :param df_ideal: ideal data
        :param df_test: test data
        """
        self.dataFrame_train = df_train
        self.dataFrame_ideal = df_ideal
        self.dataFrame_test = df_test

    def visualize_data_and_deviations(self, func_x_max_dev:pd.DataFrame, function_colors):
        """
        Visualisation of the chosen and unchosen functions, all train data, all deviation zones 
        and test data with its matched function color (if it matched)

        :param func_x_max_dev: the chosen functions matched with there individual deviation
        :param function_colors: color for the functions, last color for unmatchend test data and unchosen functions
        """

        # Plot aspect ratio
        plt.figure(figsize=(15, 10))
    
        # Plot ideal functions
        self._plot_ideal_functions(func_x_max_dev, function_colors)
    
        # Plot training data
        self._plot_training_data()
    
        # Plot test data
        self._plot_test_data(func_x_max_dev['func_id'], function_colors)
    
        # Visualize everything
        plt.xlabel('X')
        plt.ylabel('Y')
        plt.title('Data Visualization with Deviations')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, alpha=0.3)
        plt.xlim(-20, 20)
        plt.ylim(-20, 20)
        plt.tight_layout()
        plt.show()

    def darken_color(self, hex_color, factor):
        """
        Darken the color by muliplying it with a factor less than 1 (could also 
        be used to enlight the color with a multiplier higher than 1)

        :param hex_color: the color to manipulate
        :param factor: multiplier
        :return: darker color
        """

        # Convert hex to rgb
        rgb = mcolors.hex2color(hex_color)
        
        # Darken rgb by multiplying with factor
        darkened_rgb = tuple([channel * factor for channel in rgb])
        
        # Convert hex back to rgb
        darkened_hex = mcolors.to_hex(darkened_rgb)
        
        return darkened_hex
    

    def _plot_function_with_derivativ_area(self, x,y, max_deviation, function_color, text):
        """
        Plotting a function by x and y coordinates + an deviation area around it set by max_deviation

        :param x: x values of the function
        :param y: y values of the function
        :param max_deviation: size of deviation area around the function (for one side)
        :param function_color: display color of the function and its deviation area
        :param text: text to the explanation fiel on the side
        """

        # Calculate the derivative
        y_derivative = np.gradient(y, x)
    
        # Width of the orthogonal zone
        z = max_deviation
    
        # Calculate the normal vectors
        normal_x = -y_derivative / np.sqrt(1 + y_derivative**2)
        normal_y = 1 / np.sqrt(1 + y_derivative**2)
    
        # Calculate the upper and lower bounds
        x_upper = x + z * normal_x
        y_upper = y + z * normal_y
    
        x_lower = x - z * normal_x
        y_lower = y - z * normal_y

    
        # Create the contour of the zone
        x_fill = np.concatenate([x_upper, x_lower[::-1]])
        y_fill = np.concatenate([y_upper, y_lower[::-1]])
    
        plt.plot(x, y, label=text, color= function_color, linewidth=2)
        plt.fill(x_fill, y_fill, color= function_color, alpha=0.4)
    
    def _plot_training_data(self):
        """
        Scatters all the trainings data in gray and low alpha
        """
        # Boolean helper variable to only print the label one time
        label_printed = False
        # Go though the whole dataFrame_train and scatter each dot in gray 
        for col in self.dataFrame_train.columns[1:]:  # Assuming first column is 'X'
            if label_printed == True:
                plt.scatter(self.dataFrame_train['X'], self.dataFrame_train[col], alpha=0.2, s=20, c='gray')
            else:    
                plt.scatter(self.dataFrame_train['X'], self.dataFrame_train[col], alpha=0.2, label=f'Training data', s=20, c='gray')
                label_printed = True

    
    def _plot_test_data(self, chosen_functions, function_colors):
        """
        Scatter the matched and unmatched test data from the chosen functions, unmatched test data will be displayed in gray

        :param chosen_functions: the chosen function from the ideal function data set in order of the function_colors param
        :param function_colors: the function colors in order of the chosen_function param
        """

        # 5 lists for (x, y) pairs
        array_xy = [[] for _ in range(len(chosen_functions)+1)]  
    
        for _, row in self.dataFrame_test.iterrows():
            x, y = row['X (test func)'], row['Y (test func)']
            func_num = row['No. of ideal func']
            
            # Filter for chosen and unchosen test data
            if pd.notna(func_num):
                if func_num in chosen_functions.values:
                    index = chosen_functions[chosen_functions == func_num].index[0]
                    array_xy[index].append((x,y))         
            else:
                array_xy[4].append((x,y))  
                
        # Scatter matched test data
        for index in range(len(array_xy)-1):
            plt.scatter([pair[0] for pair in array_xy[index]], 
                        [pair[1] for pair in array_xy[index]], 
                        color= self.darken_color(function_colors[index],0.95), 
                        label='Matched Test Data', 
                        s=30)
        
        # Scatter unmatched test data
        plt.scatter([pair[0] for pair in array_xy[len(array_xy)-1]], 
                    [pair[1] for pair in array_xy[len(array_xy)-1]], 
                    color='gray', 
                    label='Unmatched Test Data', 
                    s=30)
    
    def _plot_ideal_functions(self, func_x_max_dev:pd.DataFrame, function_colors):
        """
        Scatter the matched and unmatched test data from the chosen functions, unmatched test data will be displayed in gray

        :param func_x_max_dev: the chosen functions matched with there individual deviation
        :param function_colors: the function colors in order of the chosen function from func_x_max_dev param
        """
        # Plot ideal functions
        x = self.dataFrame_ideal['X']
        for i in range(1, len(self.dataFrame_ideal.columns)):  # Start from column index 1
            y = self.dataFrame_ideal.iloc[np.searchsorted(self.dataFrame_ideal['X'], x), i]
            
            # Find the chosen functions
            if i in func_x_max_dev['func_id'].values:
                index = func_x_max_dev[func_x_max_dev['func_id'] == i].index[0]
                self._plot_function_with_derivativ_area(x,y,
                                                        func_x_max_dev['max_div'].at[index], 
                                                        function_colors[index], 
                                                        f'Chosen function {i}')
            # Unchosen function are displayed in gray
            else:
                plt.plot(x, y, color='gray', linewidth=1, alpha=0.2)