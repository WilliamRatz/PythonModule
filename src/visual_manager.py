import numpy as np
import pandas as pd

from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, GlyphRenderer, HoverTool, Legend, LegendItem
from bokeh.colors import RGB

from src.base_manager import BaseManager 

class VisualManager(BaseManager):
    def __init__(self, df_train: pd.DataFrame, df_ideal: pd.DataFrame, df_test: pd.DataFrame) -> None:
        """
        Initialize with dataframes and Bokeh configuration
        
        :param df_train: train DataFrame
        :param df_ideal: ideal DataFrame
        :param df_test: test DataFrame
        """
        # Initialize parent class.
        super().__init__()

        # Store the train DataFrame.
        self.dataFrame_train = df_train
        # Store the ideal DataFrame.
        self.dataFrame_ideal = df_ideal
        # Store the test DataFrame.
        self.dataFrame_test = df_test
        
        # Change the status to "INITIALIZED".
        self.set_status("INITIALIZED")

    def visualize_data_and_deviations(self, func_dev_df: pd.DataFrame, func_colors:list[str]) -> None:
        """
        Creation of an interactive visualization.
        
        :param func_dev_df: DataFrame with chosen functions and their deviations
        :param func_colors: List of colors for each function
        """
        
        # Change the status to "PLOT_GENERATION".
        self.set_status("PLOT_GENERATION")
        
        # Create the main figure.
        p = figure(
            title="Data Visualization with Deviation Areas",
            width=1100,
            height=900,
            x_range=(-20, 20),
            y_range=(-40, 40),
            tools="pan,wheel_zoom,box_zoom,reset,save",
            toolbar_location="above"
        )
        
        # Add the grid and axis label.
        p.grid.grid_line_alpha = 0.3
        p.xaxis.axis_label = "X"
        p.yaxis.axis_label = "Y"
        
        # Create a variable for the legend items.
        legend_items = []

        # Creation of a renderer to enable titels in the legend.
        # There is no other possiblity in Bokeh.
        dummy_renderer = p.scatter(x=[9999], y=[9999], size=0, alpha=0, marker='circle', visible=False)

        # Add a legend titel for chosen functions.
        legend_items.append(LegendItem(label="--- Chosen Functions ---", renderers=[dummy_renderer]))

        # Plot the ideal functions.
        # Add the ideal functions to the legend.
        legend_items += self._plot_ideal_functions(p, func_dev_df, func_colors)

        # Add a legend titel for test data.
        legend_items.append(LegendItem(label="--- Test Data ---", renderers=[dummy_renderer]))

        # Plot the test data.
        for label, renderer in self._plot_test_data(p, func_dev_df['func_id'], func_colors):
            # Add the test data to the legend.
            legend_items.append(LegendItem(label=label, renderers=[renderer]))

        # Add a legend titel for others.
        legend_items.append(LegendItem(label="--- Others ---", renderers=[dummy_renderer]))

        # Plot the training data.
        # Add the training data to the legend.
        legend_items.append(LegendItem(label="Training Data", renderers=[self._plot_training_data(p)]))
        
        # Filling of the legend with all items.
        legend = Legend(items=legend_items)

        # Set the font size of the text inside the legend.
        legend.label_text_font_size = "10pt"

        # Remove the default legend from Bokeh.
        p.legend.visible = False

        # Add the custom legend next to the plot.
        p.add_layout(legend, 'right')
        
        # Add a hover possibility.
        hover = HoverTool(tooltips=[
            ("Type", "@type"),
            ("X", "@x"),
            ("Y", "@y"),
            ("Function", "@func")
        ])
        p.add_tools(hover)
        
        # Change the status to "RENDERING".
        self.set_status("RENDERING")

        # Visualize the whole plot.
        show(p)

        # Change the status to "IDLE".
        self.set_status("IDLE")

    def darken_color(self, hex_color:str, factor:float) -> str:
        """
        Convert hex color to a darker version

        :param hex_color: Hex color code
        :param factor: Multiplier factor
        :return: Changed color
        """
        # Convert hex color to RGB tuple.
        rgb = RGB.from_hex_string(hex_color)

        # Darken the color by multiplying each component by the factor.
        r = int(rgb.r * factor)
        g = int(rgb.g * factor)
        b = int(rgb.b * factor)
    
        darkened = RGB(r,g,b)

        # Return darkened hex color.
        return darkened.to_hex()

    def _plot_ideal_functions(self, plot, func_dev_df, func_colors) -> list:
        """
        Plot the ideal functions with its deviation bands.

        :param plot: The plot object where the functions will be visualized.
        :param func_dev_df: DataFrame containing the maximum deviation for each function.
        :param func_colors: List of colors to use for each function's plot.
        :return: List of legend items.
        """

        # Extract the X values from the ideal data.
        x = self.dataFrame_ideal['X'].values

        # List to store the legend items.
        legend_items = []

        # Loop through each function (except the first column).
        for i in range(1, len(self.dataFrame_ideal.columns)):
            # Extract Y values for the function.
            y = self.dataFrame_ideal.iloc[:, i].values

            # The function ID is the column index.
            func_id = i
            
            # Check if this function has deviation data.
            if func_id in func_dev_df['func_id'].values:
                # Find the index of the function in the deviation DataFrame.
                idx = func_dev_df[func_dev_df['func_id'] == func_id].index[0]
                
                # Get the max deviation value for this function.
                max_dev = func_dev_df['max_div'].iat[idx]
                
                # Get the color for the function.
                color = func_colors[idx]

                # Reverse X values for the band.
                band_x = np.append(x, x[::-1])

                # Create the Y values for the deviation band.
                band_y = np.append(y + max_dev, (y - max_dev)[::-1])
                
                # Plot the function line.
                line_renderer = plot.line(
                    x, y,
                    line_width=2,
                    line_color=color
                )
                
                # Plot the deviation range.
                patch_renderer = plot.patch(
                    band_x, band_y,
                    fill_color=color,
                    fill_alpha=0.2,
                    line_alpha=0
                )

                # Append text for the legend.
                legend_items.append(LegendItem(label=f"Function {func_id}", 
                                               renderers=[patch_renderer, line_renderer])
            )
            else:
                # Plot the unchosen functions.
                plot.line(
                    x, y,
                    line_width=1,
                    line_color='gray',
                    line_alpha=0.2
                )

        # Return the list of legend items.
        return legend_items

    def _plot_training_data(self, plot) -> GlyphRenderer:
        """
        Scatters all the trainings data in gray and low alpha.

        :param plot: The plot object where the training data will be visualized.
        :return: Renderer of the scatter.
        """

        # Creation of the source object to hover.
        source = ColumnDataSource(data={
            'x': [],
            'y': [],
            'type': []
        })
        
        # Loop through each column in the training DataFrame.
        for col in self.dataFrame_train.columns[1:]:

            # Extraact data to visualize on plot.
            new_data = {
                'x': self.dataFrame_train['X'].values,
                'y': self.dataFrame_train[col].values,
                'type': ['Training Data'] * len(self.dataFrame_train)
            }

            # Add the new data to the data source.
            source.stream(new_data)
        
        # Scatter the training data.
        renderer = plot.scatter(
            'x', 'y',
            source=source,
            size=5,
            alpha=0.3,
            color='gray',
            legend_label='Training Data',
            marker="circle"
        )

        # Return the renderer.
        return renderer

    def _plot_test_data(self, plot, chosen_functions, func_colors) -> list:
        """
        Plot test data with matched/unmatched coloring based on ideal function matches.

        :param plot: The plot object where the test data will be visualized.
        :param chosen_functions: List of function IDs that are chosen as ideal functions.
        :param func_colors: List of colors corresponding to each function for visualization.
        :return: List of legend items.
        """
        # List to store legend items for matched/unmatched data.
        legend_items = []

        # Loop through each chosen function to plot matched test data points.
        for i, func_id in enumerate(chosen_functions):
            # Mask for the test data points matching the current function.
            mask = self.dataFrame_test['No. of ideal func'] == func_id

            # Get matched test data.
            df_matched = self.dataFrame_test[mask]
            
            # Create a data source for matched points.
            source = ColumnDataSource(data={
                'x': df_matched['X (test func)'],
                'y': df_matched['Y (test func)'],
                'type': ['Matched Test Data'] * len(df_matched),
                'func': [f'Function {func_id}'] * len(df_matched)
            })
            
            # Scatter matched test data points.
            renderer = plot.scatter(
                'x', 'y',
                source=source,
                size=8,
                # Darken the function color.
                color=self.darken_color(func_colors[i], 0.9),
                legend_label=f'Matched to {func_id}',
                marker="circle"
            )

            # Add to the legend.
            legend_items.append((f'Test Data matched to Func. {func_id}', renderer))

        # Extract the unmatched test data points that don't correspond to any ideal function.
        unmatched = self.dataFrame_test[self.dataFrame_test['No. of ideal func'].isna()]

        # Create a data source for the unmatched points.
        unmatched_source = ColumnDataSource(data={
            'x': unmatched['X (test func)'],
            'y': unmatched['Y (test func)'],
            'type': ['Unmatched Test Data'] * len(unmatched),
            'func': ['None'] * len(unmatched)
        })
        
        # Scatter the unmatched test data points.
        unmatched_renderer = plot.scatter(
            'x', 'y',
            source=unmatched_source,
            size=8,
            color='#898f8b',
            legend_label='Unmatched Test Data',
            marker="circle"
        )

        # Add to the legend.
        legend_items.append(("Unmatched Test Data", unmatched_renderer))

        # Return the list of legend items for test data.
        return legend_items