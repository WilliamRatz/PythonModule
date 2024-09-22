import src.sql_manager as sql_mgm
import numpy as np

sql_mgm.createDatabase()

#sql_mgm.load_idealCSV("./data/ideal.csv")
#sql_mgm.load_testCSV("./data/test.csv")
#sql_mgm.load_trainCSV("./data/train.csv")


dataFrame_ideal = sql_mgm.load_table("ideal_db")
dataFrame_test = sql_mgm.load_table("test_db")
dataFrame_train = sql_mgm.load_table("train_db")

