import sqlalchemy
import pymysql
import sqlalchemy as db

db_engine = db.create_engine("mysql+pymysql://root:password@localhost/MySQL")

def print_table(table_name):
    print("sqlalchemy: {}".format(sqlalchemy.__version__))
    meta_data = db.MetaData()

    # connect to database and get the table
    with db_engine.connect() as connection:
        table = db.Table(table_name, meta_data, autoload_with=db_engine)
        select_statement = db.select(table)
    	
        # get all data and print each row
        result = connection.execute(select_statement).fetchall() 
        for row in result:
            print(row)

def insert_train_db(x, y1, y2, y3, y4):
    connection = db_engine.connect()
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
        connection.close()

def insert_ideal_db(x, y_values):
    connection = db_engine.connect()
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
        connection.close()

def insert_test_db(x_test, y_test, delta_y_test, no_ideal_func):
    connection = db_engine.connect()
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
        connection.close()



def createDatabase():
    # get connection object
    connection = db_engine.connect()
    
    try:
        # get meta data object
        meta_data = db.MetaData()

        # create train_db table Y1-Y4
        y_columns = [
            db.Column(f'Y{i} (training func)', db.DOUBLE_PRECISION) for i in range(1, 5)
        ]

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
            db.Column('X (test func)', db.DOUBLE_PRECISION, primary_key=True, autoincrement=False, nullable=True),
            db.Column('Y (test func)', db.DOUBLE_PRECISION),
            db.Column('Delta Y (test func)', db.DOUBLE_PRECISION),
            db.Column('No. of ideal func', db.DOUBLE_PRECISION)
        )

        # create train_db table and stores the information in metadata
        meta_data.create_all(db_engine)

        # on success return True
        return True

    except Exception as e:
        print(f"Error while database creation: {e}")
        connection.rollback()
        return False

    finally:
        # close connection
        connection.close()