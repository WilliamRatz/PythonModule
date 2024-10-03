import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

class VisualManger:

    def __init__(self, df_train, df_ideal, df_test):
        self.dataFrame_train = df_train
        self.dataFrame_ideal = df_ideal
        self.dataFrame_test = df_test


    def plot_function_with_derivativ_area(self, x,y, function_color, text):
        # Calculate the derivative
        y_derivative = np.gradient(y, x)
    
        # Width of the orthogonal zone
        z = self.max_deviation
    
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
    
        plt.plot(x, y, label=text, color= function_color, linewidth=2, alpha=0.8)
        plt.fill(x_fill, y_fill, color= function_color, alpha=0.4)
    
    def darken_color(self, hex_color, factor):
        # convert hex to rgb
        rgb = mcolors.hex2color(hex_color)
        
        # darken rgb by multiplying with factor
        darkened_rgb = tuple([channel * factor for channel in rgb])
        
        # convert hex back to rgb
        darkened_hex = mcolors.to_hex(darkened_rgb)
        
        return darkened_hex
    
    def plot_training_data(self):
        for col in self.dataFrame_train.columns[1:]:  # Assuming first column is 'X'
            plt.scatter(self.dataFrame_train['X'], self.dataFrame_train[col], alpha=0.2, label=f'Training data', s=20, c='gray')
    
    def plot_test_data(self, chosen_functions, function_colors):
        array_xy = [[] for _ in range(len(chosen_functions)+1)]  # 4 Listen für (x, y) Paare
    
        for _, row in self.dataFrame_test.iterrows():
            x, y = row['X (test func)'], row['Y (test func)']
            func_num = row['No. of ideal func']
            
            if pd.notna(func_num):
                if func_num in chosen_functions:
                    array_xy[chosen_functions.index(func_num)].append((x,y))         
            else:
                array_xy[4].append((x,y))  
                
        # plot matched test data
        for index in range(len(array_xy)-1):
            plt.scatter([pair[0] for pair in array_xy[index]], 
                        [pair[1] for pair in array_xy[index]], 
                        color= self.darken_color(function_colors[index],0.95), 
                        label='Matched Test Data', 
                        s=30)
        
        # plot unmatched test data
        plt.scatter([pair[0] for pair in array_xy[len(array_xy)-1]], 
                    [pair[1] for pair in array_xy[len(array_xy)-1]], 
                    color='#808080', 
                    label='Unmatched Test Data', 
                    s=30)
    
    def plot_ideal_functions(self, chosen_functions, max_deviation, function_colors):
        # Plot ideal functions
        x = self.dataFrame_ideal['X']
        for i in range(1, len(self.dataFrame_ideal.columns)):  # Start from column index 1
            y = self.dataFrame_ideal.iloc[np.searchsorted(self.dataFrame_ideal['X'], x), i]
            
            if i in chosen_functions:
                self.plot_function_with_derivativ_area(x,y,max_deviation[chosen_functions.index(i)], function_colors[chosen_functions.index(i)], 'Chosen function {i}')
    
            else:
                plt.plot(x, y, color='gray', linewidth=1, alpha=0.2)
    
    def visualize_data_and_deviations(self, chosen_functions, max_deviation, function_colors):
        plt.figure(figsize=(15, 10))
    
        # Plot ideal functions
        self.plot_ideal_functions(chosen_functions, max_deviation, function_colors)
    
        # Plot training data
        self.plot_training_data(self.dataFrame_train)
    
        # Plot test data
        self.plot_test_data(self.dataFrame_test, chosen_functions, function_colors)
    
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