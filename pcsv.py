import read_csvs
import pandas as pd
import os
import time

NAME = "output"

def missing_data(df_miss, con):
    '''get missing data'''
    list_add = [()]
    for hour, minute in zip(df_miss['date'].dt.hour, df_miss['date'].dt.minute):
        list_add.append((hour, minute))
    con = sorted(set(list_add))

    #compare hour and minute list to the list(set())
    hours = [int(f"{hour:02d}") for hour in range(24)]
    print(hours)
    minutes = list(range(0, 60))
    missing_sets = []
    for hour in hours:
        for minute in minutes:
            if (hour, minute) not in con:
                missing_sets.append((hour, minute))
    print("-------------------------------------------------")

    my_dict_list = [{'hour': tup[0], 'minute missing': tup[1]} for tup in missing_sets]
    for item in my_dict_list:
        print(item)

def df_general(df_generaltotal):
    '''df general'''
    print(df_generaltotal)
    print("-------------------------------------------------")
    df3 = df_generaltotal.sort_values(by=['dfa_data_timestamp'])
    print(df3[['dfa_data_timestamp', 'dfa_toll_fk', 'dfa']])

    df3['dfa_data_timestamp'] = pd.to_datetime(df3['dfa_data_timestamp'])
    #create new columns
    df3['date'] = df3['dfa_data_timestamp'].dt.date
    df3['hour'] = df3['dfa_data_timestamp'].dt.hour
    df3['minute'] = df3['dfa_data_timestamp'].dt.minute
    #convert column
    df3['date'] = df3['dfa_data_timestamp']
    df3['date'] = pd.to_datetime(df3['dfa_data_timestamp'])

    df4 = df3[['dfa_data_timestamp', 'dfa_toll_fk', 'dfa', 'date', 'hour', 'minute']]
    df105 = df4[df4['dfa_toll_fk'] == 101]
    df106 = df4[df4['dfa_toll_fk'] == 102]
    df107 = df4[df4['dfa_toll_fk'] == 103]

    missing_data(df105, "105")
    missing_data(df106, "106")
    missing_data(df107, "107")

def main() -> None:
    '''main'''
    date = input("Enter date: yyyy-mm-dd: ")
    current_folder = os.path.dirname(__file__)
    print(current_folder + "/data/" + str(date))
    date_exist: bool = os.path.exists(current_folder + "/data/" + str(date))
    if date_exist:
        #bindings rust
        data = read_csvs.create_new_output_csv_with_header(str(date))
        print(data)
        df_output = pd.read_csv(f'{NAME}.csv', header=0)
        df_output_drop = df_output.drop_duplicates('dfa', keep='last')
        df_general(df_output_drop)
    else:
        print("date doesn't exist")

if __name__ == "__main__":
    start = time.time()
    main()
    duration = round((time.time()- start)/60, 3)
    print(f'execution time {duration} minutes')
    print(round(time.time()- start, 2), " secs")
