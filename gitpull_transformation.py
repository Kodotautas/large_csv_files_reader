'''
Uzduotis:
Paimti github'o pull request'ų dataset'ą: https://www.kaggle.com/stephangarland/ghtorrent-pu... (92.53 GB),
suprocessinti ir padėti į savo nuožiūrą pasirinktą warehouse'ą.
Ant duomenų, panaudojant bet kokį pasirinktą įrankį, padaryti primityvią analitiką, kaip pvz.
"Iš TOP 100 populiauriausių Python repositorijų, surasti top 3 daugiausiai pull request'ų darančius vartotojus"
Technologijos ir prog. kalbos: pasirinktinai
Pateikimo būdas: atsiųsti kodą ar/ir pristatymas online
'''

import os
from os import listdir
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
from sqlalchemy import create_engine

# Authenticate API
# For API token read doc: https://github.com/Kaggle/kaggle-api#api-credentials
api = KaggleApi()
api.authenticate()

# Download .csv from Kaggle
print('Downloading .csv files...  Use your time wisely')
data_zip = api.dataset_download_files('stephangarland/ghtorrent-pull-requests', unzip=True)
print('Dowload complete!')

# Get the current working directory
cwd = os.getcwd()


def find_csv_filenames(cwd=cwd, prefix="ghtorrent"):
    '''
    :param cwd: curent working directory
    :param prefix: what file name should be at the beggining
    :return: list of files which match filter
    '''
    filenames = listdir(cwd)
    files = [cwd + "\\" + filename for filename in filenames if filename.startswith(prefix)]
    return files


files = find_csv_filenames()

github_local_db = create_engine('sqlite:///github_local_database.db')

# Push data to database
print('Loading to database...')
i = 0
j = 1
for file in files:
    for df in pd.read_csv(file, chunksize=500000, iterator=True):
        #Filter only Python language related data
        df = df[df['language'] == 'Python']
        df.index += j
        i += 1
        df.to_sql('pull_requests', github_local_db, if_exists='append')
        j = df.index[-1] + 1
        print("Readed billions rows: ", (j - 1) / 1000000000,
              "current file: ", file)

#Query to find top pull requesters
SQL = '''
      SELECT DISTINCT repo, actor_login, count(actor_login)
      FROM pull_requests
      GROUP BY repo, actor_login
      ORDER BY count(repo) desc
      LIMIT 3;
      '''

print('Loading query....')
results = pd.read_sql_query(SQL, github_local_db)
print(results)
