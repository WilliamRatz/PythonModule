import sqlalchemy as db
import pandas as pd

class DatabaseManager:
    def __init__(self, db_path):
        '''
        Creats/Loads database engine

        :param db_path: path to the database
        '''
        # Create engine so it can be used in the whole class
        self.db_engine = db.create_engine(f'sqlite:///{db_path}')

    def load_table(self, table_name):
        '''
        Loads table into a pandas data frame

        :param table_name: name of the table to load
        :return: panda data frame of table
        '''

        table = db.Table(table_name, db.MetaData(), autoload_with=self.db_engine)
        select_statement = db.select(table)
        # Connect to database and get the table
        with self.db_engine.connect() as connection:
            result = connection.execute(select_statement)

            # Fetch all results into a list of tuples
            rows = result.fetchall()

            # Convert table into a dataframe
            column_names = table.columns.keys()
            df = pd.DataFrame(rows, columns=column_names)

            return df

    def csv_2DArray(self, directory):
        '''
        Read csv file into a pandas data frame

        :param directory: directory of csv file
        :return: panda data frame of csv file
        '''
        return pd.read_csv(directory)

    def import_trainCSV(self, directory):
        '''
        Import the train data from the train.csv into the database
        
        :param directory: directory of csv file
        :return: size of successfull added records
        '''
        counter = 0
        train_df = self.csv_2DArray(directory)
        for ind in train_df.index:
            if self.trainDB_add_record(train_df['x'][ind], train_df['y1'][ind], train_df['y2'][ind], train_df['y3'][ind], train_df['y4'][ind]): 
                counter += 1
        
        # Return the amount of records that has been added
        return counter

    def import_idealCSV(self, directory):
        '''
        Import the ideal data from the ideal.csv into the database

        :param directory: directory of csv file
        :return: size of successfull added records
        '''
        counter = 0
        ideal_df = self.csv_2DArray(directory)
        for ind in ideal_df.index:
            x_value = ideal_df['x'][ind]
            y_values = ideal_df.loc[ind, 'y1':'y50'].values
            # on success increase the counter by one
            if self.idealDB_add_record(x_value, y_values): 
                counter += 1
        
        # Return the amount of records that has been added
        return counter


    def trainDB_add_record(self, x, y1, y2, y3, y4):
        '''
        Add a record to the train table in the database

        :param x: X value
        :param y1: Y1 (training func) value
        :param y2: Y2 (training func) value
        :param y3: Y3 (training func) value
        :param y4: Y4 (training func) value
        :param directory: directory of csv file
        :return: BOOL if successfull
        '''
        connection = self.db_engine.connect()
        try:
            # Creation SQL statement with placeholder
            sql = db.text("""
                INSERT INTO train_db 
                (`X`, `Y1 (training func)`, `Y2 (training func)`, `Y3 (training func)`, `Y4 (training func)`)
                VALUES (:x, :y1, :y2, :y3, :y4)
            """)

            # Parameter with input data
            params = {
                'x': x,
                'y1': y1,
                'y2': y2,
                'y3': y3,
                'y4': y4
            }

            # Execute SQL statement
            connection.execute(sql, params)
            connection.commit()
            return True

        except Exception as e:
            print(f"Error while INSERT operation in train_db: {e}")
            connection.rollback()
            return False

        finally:
            # Close connection
            connection.close()

       
        
    def idealDB_add_record(self, x, y_values):
        '''
        Add a record to the train table in the database

        :param x: X value
        :param y_values: array containing all y indexes from 1 to 50
        :param directory: directory of csv file
        :return: BOOL if successfull
        '''
        connection = self.db_engine.connect()
        try:
            # Create column name string for SQL statement
            columns = ['`X`'] + [f'`Y{i} (ideal func)`' for i in range(1, 51)]
            column_string = ', '.join(columns)

            # Create value placeholder
            value_placeholders = [':x'] + [f':y{i}' for i in range(1, 51)]
            value_string = ', '.join(value_placeholders)

            # Creation of SQL statement with placeholder
            sql = f"""INSERT INTO ideal_db ({column_string}) VALUES ({value_string})"""

            # Parameter with input data
            params = {'x': x}
            params.update({f'y{i+1}': y for i, y in enumerate(y_values)})

            # Execute SQL statement
            connection.execute(db.text(sql), params)
            connection.commit()
            return True

        except Exception as e:
            print(f"Error while INSERT operation in ideal_db: {e}")
            connection.rollback()
            return False

        finally:
            # Close connections
            connection.close()

    
    def testDB_add_record(self, x_test, y_test, delta_y_test, no_ideal_func):
        '''
        Add a record to the test table in the database

        :param x_test: X value
        :param y_test: Y (test func) value
        :param delta_y_test: Delta Y (test func) value
        :param no_ideal_func: No. of ideal func value
        :return: BOOL if successfull
        '''
        connection = self.db_engine.connect()
        try:
            # Creation of SQL statement with placeholder
            sql = db.text("""
                INSERT INTO test_db 
                (`X (test func)`, `Y (test func)`, `Delta Y (test func)`, `No. of ideal func`)
                VALUES (:x_test, :y_test, :delta_y_test, :no_ideal_func)
            """)

            # Parameter with input data
            params = {
                'x_test': x_test,
                'y_test': y_test,
                'delta_y_test': delta_y_test,
                'no_ideal_func': no_ideal_func
            }

            # Execute SQL statement
            connection.execute(sql, params)
            connection.commit()
            return True

        except Exception as e:
            print(f"Error while INSERT operation in test_db: {e}")
            connection.rollback()
            return False

        finally:
            # Close connections
            connection.close()
    

    def createDatabase(self):
        '''
        Creates all needed database tabels at the choosen direction, if not already exist

        :return: true if successfull
        '''
        # Get connection object
        connection = self.db_engine.connect()

        try:
            # Get meta data object
            meta_data = db.MetaData()

            # Create train_db table Y1-Y4
            y_columns = [
                db.Column(f'Y{i} (training func)', db.DOUBLE_PRECISION) for i in range(1, 5)
            ]
            
            # Combine the X coulumn with the Y1 to Y4 columns
            train_db = db.Table(
                'train_db',
                meta_data,
                db.Column('X', db.DOUBLE_PRECISION, primary_key=True, autoincrement=False),
                *y_columns,
            )

            # Create ideal_db table Y1-Y50
            y_columns = [
                db.Column(f'Y{i} (ideal func)', db.DOUBLE_PRECISION) for i in range(1, 51)
            ]

            # Combine the X coulumn with the Y1 to Y50 columns
            ideal_db = db.Table(
                'ideal_db',
                meta_data,
                db.Column('X', db.DOUBLE_PRECISION, primary_key=True, autoincrement=False, nullable=True,),
                *y_columns,
            )

            # Create test_db table
            test_db = db.Table(
                'test_db',
                meta_data,
                db.Column('Primary Key', db.DOUBLE_PRECISION, primary_key=True, autoincrement=True, nullable=True),
                db.Column('X (test func)', db.DOUBLE_PRECISION),
                db.Column('Y (test func)', db.DOUBLE_PRECISION),
                db.Column('Delta Y (test func)', db.DOUBLE_PRECISION),
                db.Column('No. of ideal func', db.DOUBLE_PRECISION)
            )

            # Create train_db table and stores the information in metadata
            meta_data.create_all(self.db_engine)

            # On success return True
            return True

        except Exception as e:
            print(f"Error while database creation: {e}")
            connection.rollback()
            return False

        finally:
            # Close connection
            connection.close()