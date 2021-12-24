import os
from os import listdir
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
from sqlalchemy import create_engine
import time

#Start measuring time
start_time = time.time()

# Authenticate API
# For API token read doc: https://github.com/Kaggle/kaggle-api#api-credentials
api = KaggleApi()
api.authenticate()

# # Download .csv from Kaggle
print('Downloading .csv files...  Use your time wisely')
data_zip = api.dataset_download_files('stephangarland/ghtorrent-pull-requests', unzip=True)
print('Dowload complete!')

# Get the current working directory
cwd = os.getcwd()


def find_csv_filenames(cwd=cwd, prefix="ghtorrent", suffix='.csv'):
    """
    :param cwd: curent working directory
    :param prefix: what file name should be at the beggining
    :param suffix: filename ends with .csv
    :return: list of files which match filter
    """
    filenames = listdir(cwd)
    files = [cwd + "\\" + filename for filename in filenames if
             filename.startswith(prefix) and filename.endswith(suffix)]
    return files


files = find_csv_filenames()

github_local_db = create_engine('sqlite:///github_local_database.db')

# Push data to database
print('Loading to database...')

i = 0
j = 1
size = 500000
for file in files:
    thead = pd.read_csv(file, nrows=3)
    dtypes = dict(zip(thead.columns.values, ['str', 'int32', 'int32', 'str', 'str', 'str',
                                             'str', 'int32', 'int32', 'int32']))
    for df in pd.read_csv(file, chunksize=size, iterator=True, dtype=dtypes):
        # Get only specific columns
        df = df[['actor_login', 'repo', 'language']]
        # Filter only Python language related data
        df = df[df['language'] == 'Python']
        df.index += j
        i += 1
        df.to_sql('pull_requests', github_local_db, if_exists='append')
        j = df.index[-1] + 1
        print("Readed rows in millions: ", (i * size) / 1000000,
              "current file: ", file)

# Query to find top pull requesters
SQL = '''
      SELECT DISTINCT repo AS repository, actor_login AS user, 
                      count(actor_login) AS pull_request_count
      FROM pull_requests
      GROUP BY repo, actor_login
      ORDER BY count(repo) desc
      LIMIT 3;
      '''

print('Loading query....')
results = pd.read_sql_query(SQL, github_local_db)
print(results)

print("--- %s seconds ---" % (time.time() - start_time))