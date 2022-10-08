import functions as f
import json, sqlite3
import pandas as pd

def step_1():
    """
        Get the raw data from MagicEden via API
    """
    activities = f.get_activities("degods")

    # Create or open an existing file and write a dict in the file
    with open('data/activities.json', 'w') as outfile:
        json.dump(activities, outfile)


def step_2():
    """
        Read json from file, safe it in to a dataframe, filter dublicates and safe it to DB
    """
    # Opening JSON file
    f = open('data/activities.json')  # 'data/activities copy.json'
    # returns JSON object as a dictionary
    data = json.load(f)

    df = pd.DataFrame(data)

    # Write the dataframe "df_anon" to the database
    con = sqlite3.connect('data/database.db')
    df.to_sql(name='df_anon', con=con, if_exists="replace")
    con.close()
    



if __name__ == "__main__":
    step_2()