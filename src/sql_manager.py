import sqlalchemy as db
import pandas as pd

class DatabaseManager:
    def __init__(self, db_path):
        '''
        creats/loads database engine\n
        db_path: path to the database\n
        return: none
        '''
        # create engine so it can be used in the whole class
        self.db_engine = db.create_engine(f'sqlite:///{db_path}')

    def load_table(self, table_name):
        '''
        loads table into a pandas data frame\n
        table_name: name of the table to load\n
        return: panda data frame of table
        '''

        table = db.Table(table_name, db.MetaData(), autoload_with=self.db_engine)
        select_statement = db.select(table)
        # connect to database and get the table
        with self.db_engine.connect() as connection:
            result = connection.execute(select_statement)

            # fetch all results into a list of tuples
            rows = result.fetchall()

            # convert table into a dataframe
            column_names = table.columns.keys()
            df = pd.DataFrame(rows, columns=column_names)

            return df

    def csv_2DArray(self, directory):
        '''
        read csv file into a pandas data frame\n
        directory: directory of csv file\n
        return: panda data frame of csv file
        '''
        return pd.read_csv(directory)

    def import_trainCSV(self, directory):
        '''
        import the train data from the train.csv 
        into the database\n
        directory: directory of csv file\n
        return: size of successfull added records
        '''
        counter = 0
        train_df = self.csv_2DArray(directory)
        for ind in train_df.index:
            if self.trainDB_add_record(train_df['x'][ind], train_df['y1'][ind], train_df['y2'][ind], train_df['y3'][ind], train_df['y4'][ind]): 
                counter += 1
        
        # return the amount of records that has been added
        return counter

    def import_idealCSV(self, directory):
        '''
        import the ideal data from the ideal.csv\n 
        into the database\n
        directory: directory of csv file\n
        return: size of successfull added records
        '''
        counter = 0
        ideal_df = self.csv_2DArray(directory)
        for ind in ideal_df.index:
            x_value = ideal_df['x'][ind]
            y_values = ideal_df.loc[ind, 'y1':'y50'].values
            # on success increase the counter by one
            if self.idealDB_add_record(x_value, y_values): 
                counter += 1
        
        # return the amount of records that has been added
        return counter


    def trainDB_add_record(self, x, y1, y2, y3, y4):
        '''
        add a record to the train table in the database\n
        x: X value\n
        y1: Y1 (training func) value\n
        y2: Y2 (training func) value\n
        y3: Y3 (training func) value\n
        y4: Y4 (training func) value\n
        directory: directory of csv file\n
        return: BOOL if successfull
        '''
        connection = self.db_engine.connect()
        try:
            # creation SQL statement with placeholder
            sql = db.text("""
                INSERT INTO train_db 
                (`X`, `Y1 (training func)`, `Y2 (training func)`, `Y3 (training func)`, `Y4 (training func)`)
                VALUES (:x, :y1, :y2, :y3, :y4)
            """)

            # parameter with input data
            params = {
                'x': x,
                'y1': y1,
                'y2': y2,
                'y3': y3,
                'y4': y4
            }

            # execute SQL statement
            connection.execute(sql, params)
            connection.commit()
            return True

        except Exception as e:
            print(f"Error while INSERT operation in train_db: {e}")
            connection.rollback()
            return False

        finally:
            # close connection
            connection.close()

       
        
    def idealDB_add_record(self, x, y_values):
        '''
        add a record to the train table in the database\n
        x: X value\n
        y_values: array containing all y indexes from 1 to 50\n
        directory: directory of csv file\n
        return: BOOL if successfull
        '''
        connection = self.db_engine.connect()
        try:
            # create column name string for SQL statement
            columns = ['`X`'] + [f'`Y{i} (ideal func)`' for i in range(1, 51)]
            column_string = ', '.join(columns)

            # create value placeholder
            value_placeholders = [':x'] + [f':y{i}' for i in range(1, 51)]
            value_string = ', '.join(value_placeholders)

            # creation of SQL statement with placeholder
            sql = f"""INSERT INTO ideal_db ({column_string}) VALUES ({value_string})"""

            # parameter with input data
            params = {'x': x}
            params.update({f'y{i+1}': y for i, y in enumerate(y_values)})

            # execute SQL statement
            connection.execute(db.text(sql), params)
            connection.commit()
            return True

        except Exception as e:
            print(f"Error while INSERT operation in ideal_db: {e}")
            connection.rollback()
            return False

        finally:
            # close connections
            connection.close()

    
    def testDB_add_record(self, x_test, y_test, delta_y_test, no_ideal_func):
        '''
        add a record to the test table in the database\n
        x_test: X value\n
        y_test: Y (test func) value\n
        delta_y_test: Delta Y (test func) value\n
        no_ideal_func: No. of ideal func value\n
        return: BOOL if successfull
        '''
        connection = self.db_engine.connect()
        try:
            # creation of SQL statement with placeholder
            sql = db.text("""
                INSERT INTO test_db 
                (`X (test func)`, `Y (test func)`, `Delta Y (test func)`, `No. of ideal func`)
                VALUES (:x_test, :y_test, :delta_y_test, :no_ideal_func)
            """)

            # parameter with input data
            params = {
                'x_test': x_test,
                'y_test': y_test,
                'delta_y_test': delta_y_test,
                'no_ideal_func': no_ideal_func
            }

            # execute SQL statement
            connection.execute(sql, params)
            connection.commit()
            return True

        except Exception as e:
            print(f"Error while INSERT operation in test_db: {e}")
            connection.rollback()
            return False

        finally:
            # close connections
            connection.close()
    

    def createDatabase(self):
        '''
        creates all needed database tabels at the choosen direction
        if not already exist\n
        return: BOOL if successfull
        '''
        # get connection object
        connection = self.db_engine.connect()

        try:
            # get meta data object
            meta_data = db.MetaData()

            # create train_db table Y1-Y4
            y_columns = [
                db.Column(f'Y{i} (training func)', db.DOUBLE_PRECISION) for i in range(1, 5)
            ]
            
            # combine the X coulumn with the Y1 to Y4 columns
            train_db = db.Table(
                'train_db',
                meta_data,
                db.Column('X', db.DOUBLE_PRECISION, primary_key=True, autoincrement=False),
                *y_columns,
            )

            # create ideal_db table Y1-Y50
            y_columns = [
                db.Column(f'Y{i} (ideal func)', db.DOUBLE_PRECISION) for i in range(1, 51)
            ]

            # combine the X coulumn with the Y1 to Y50 columns
            ideal_db = db.Table(
                'ideal_db',
                meta_data,
                db.Column('X', db.DOUBLE_PRECISION, primary_key=True, autoincrement=False, nullable=True,),
                *y_columns,
            )

            # create test_db table
            test_db = db.Table(
                'test_db',
                meta_data,
                db.Column('Primary Key', db.DOUBLE_PRECISION, primary_key=True, autoincrement=True, nullable=True),
                db.Column('X (test func)', db.DOUBLE_PRECISION),
                db.Column('Y (test func)', db.DOUBLE_PRECISION),
                db.Column('Delta Y (test func)', db.DOUBLE_PRECISION),
                db.Column('No. of ideal func', db.DOUBLE_PRECISION)
            )

            # create train_db table and stores the information in metadata
            meta_data.create_all(self.db_engine)

            # on success return True
            return True

        except Exception as e:
            print(f"Error while database creation: {e}")
            connection.rollback()
            return False

        finally:
            # close connection
            connection.close()