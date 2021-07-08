import os
import pandas as pd
import Python.Data_Preprocessing.config.dir_config as prs
import Python.Data_Preprocessing.Stage_3.SQL_connection.sql_connection as SC

def upload_csv_to_db(parallel_run_settings):
    # Read file to upload
    full_path_to_file = os.path.join(parallel_run_settings['csv_path'],
                                     "Stage_3",
                                     "narrative_fine.csv")
    df = pd.read_csv(full_path_to_file)

    # Insert row values to the db
    data = df.values.tolist()
    sql = 'INSERT INTO earnings_call.Narrative_Fine VALUES({0})'
    sql = sql.format(','.join('?' * len(df.columns)))
    cnxn = SC.get_connection()
    cursor = cnxn.cursor()
    number_of_rows = cursor.executemany(sql, data)
    cnxn.commit()
    cursor.close()
    cnxn.close()

    # Update open face log
    print("Inserted " + str(len(df)) + " rows to db")


if __name__=='__main__':
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    upload_csv_to_db(parallel_run_settings=parallel_run_settings)