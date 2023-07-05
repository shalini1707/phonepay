#!/usr/bin/env python
# coding: utf-8

# In[183]:


from git import Repo
import os
import pandas as pd
import numpy as np
import mysql.connector


# In[184]:


repo_url = "https://github.com/PhonePe/pulse.git"
clone_path = r"C:\Users\SONY\Desktop\data"

if not os.path.exists(clone_path):
    os.makedirs(clone_path)

repo_path = os.path.join(clone_path, os.path.basename(repo_url).removesuffix('.git').title())

Repo.clone_from(repo_url, repo_path)

dir1 = os.path.join(repo_path, 'data')
print(dir1)


# In[185]:


def rename(dir1):
    for root, dirs, files in os.walk(dir1):
        if 'state' in dirs:
            state_dir = os.path.join(root, 'state')
            for state_folder in os.listdir(state_dir):
                # rename the state folder
                old_path = os.path.join(state_dir, state_folder)
                new_path = os.path.join(state_dir, state_folder.title().replace('-', ' ').replace('&', 'and'))
                os.rename(old_path, new_path)
    print("Renamed all sub-directories successfully")
                
# Function to extract all paths that has sub-directory in the name of 'state'

def extract_paths(dir1):
    path_list = []
    for root, dirs, files in os.walk(dir1):
        if os.path.basename(root) == 'state':
            path_list.append(root.replace('\\', '/'))
    return path_list


# In[186]:


state_dir1 = extract_paths(dir1)
state_dir1


# In[187]:


# aggerate_transaction 


# In[188]:


state_path = state_dir1[0]
state_list = os.listdir(state_path)
agg_trans_dict = {
    'State': [], 'Year': [], 'Transaction_type': [],
    'Transaction_count': [], 'Transaction_amount': []
}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for transaction_data in df['data']['transactionData']:
                    type = transaction_data['name']
                    count = transaction_data['paymentInstruments'][0]['count']
                    amount = transaction_data['paymentInstruments'][0]['amount']

                    # Appending to agg_trans_dict
                    agg_trans_dict['State'].append(state)
                    agg_trans_dict['Year'].append(year)
                    agg_trans_dict['Transaction_type'].append(type)
                    agg_trans_dict['Transaction_count'].append(count)
                    agg_trans_dict['Transaction_amount'].append(amount)
            except:
                pass

agg_trans_df = pd.DataFrame(agg_trans_dict)


# In[ ]:


#aggerate_user


# In[189]:


state_path = state_dir1[1]
state_list = os.listdir(state_path)
agg_user_dict = {
    'State': [], 'Year': [], 'Transaction_count': [], 'Percentage': []
}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for user_data in df['data']['usersByDevice']:
                    count = user_data['count']
                    percent = user_data['percentage']

                    # Appending to agg_user_dict
                    agg_user_dict['State'].append(state)
                    agg_user_dict['Year'].append(year)
                    agg_user_dict['Transaction_count'].append(count)
                    agg_user_dict['Percentage'].append(percent)
            except:
                pass

agg_user_df = pd.DataFrame(agg_user_dict)


# In[ ]:


#map transaction


# In[190]:


state_path = state_dir1[2]
state_list = os.listdir(state_path)
map_trans_dict = {
    'State': [], 'Year': [], 'District': [],
    'Transaction_count': [], 'Transaction_amount': []
}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for transaction_data in df['data']['hoverDataList']:
                    district = transaction_data['name']
                    count = transaction_data['metric'][0]['count']
                    amount = transaction_data['metric'][0]['amount']

                    # Appending to map_trans_dict
                    map_trans_dict['State'].append(state)
                    map_trans_dict['Year'].append(year)
                    map_trans_dict['District'].append(district.removesuffix(' district').title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    map_trans_dict['Transaction_count'].append(count)
                    map_trans_dict['Transaction_amount'].append(amount)
            except:
                pass

map_trans_df = pd.DataFrame(map_trans_dict)


# In[12]:


#map user


# In[191]:


state_path = state_dir1[3]
state_list = os.listdir(state_path)
map_user_dict = {
    'State': [], 'Year': [], 'District': [],
    'Registered_users': [], 'App_opens': []
}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district, user_data in df['data']['hoverData'].items():
                    reg_user_count = user_data['registeredUsers']
                    app_open_count = user_data['appOpens']

                    # Appending to map_user_dict
                    map_user_dict['State'].append(state)
                    map_user_dict['Year'].append(year)
                    map_user_dict['District'].append(district.removesuffix(' district').title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    map_user_dict['Registered_users'].append(reg_user_count)
                    map_user_dict['App_opens'].append(app_open_count)
            except:
                pass

map_user_df = pd.DataFrame(map_user_dict)


# In[ ]:


# transaction distrct wise


# In[192]:


state_path = state_dir1[4]
state_list = os.listdir(state_path)
top_trans_dist_dict = {
                        'State': [], 'Year': [], 'District': [],
                        'Transaction_count': [], 'Transaction_amount': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for district_data in df['data']['districts']:
                    
                    name = district_data['entityName']
                    count = district_data['metric']['count']
                    amount = district_data['metric']['amount']
                    # Appending to top_trans_dist_dict
                    
                    top_trans_dist_dict['State'].append(state)
                    top_trans_dist_dict['Year'].append(year)
                    top_trans_dist_dict['District'].append(name.title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    top_trans_dist_dict['Transaction_count'].append(count)
                    top_trans_dist_dict['Transaction_amount'].append(amount)
            except:
                pass

top_trans_dist_df = pd.DataFrame(top_trans_dist_dict)


# In[ ]:


#top user


# In[193]:


state_path = state_dir1[5]
state_list = os.listdir(state_path)
top_user_dist_dict = {
                        'State': [], 'Year': [], 'District': [],
                        'Registered_users': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for district_data in df['data']['districts']:
                    
                    name = district_data['name']
                    count = district_data['registeredUsers']
                    # Appending to top_user_dist_dict
                    
                    top_user_dist_dict['State'].append(state)
                    top_user_dist_dict['Year'].append(year)
                    top_user_dist_dict['District'].append(name.title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    top_user_dist_dict['Registered_users'].append(count)
            except:
                pass

top_user_dist_df = pd.DataFrame(top_user_dist_dict)


# In[ ]:


# one data frame


# In[14]:


df_list = [df for df in globals() if isinstance(globals()[df], pd.core.frame.DataFrame) and df.endswith('_df')]

df_list


# In[ ]:


# some mismatch 


# In[ ]:


##Adding Latitude and Longitude columns


# In[177]:


lat_long_df = pd.read_csv(r"C:\Users\SONY\Downloads\dist_lat_long.csv")

for df_name in df_list:
    df = globals()[df_name]
    if 'district' in df.columns:
        df = pd.merge(df, lat_long_df, on=['state', 'district'], how='left')
        globals()[df_name] = df


# In[ ]:


def add_region_column(df):
    state_groups = {
        'Northern Region': ['Jammu and Kashmir', 'Himachal Pradesh', 'Punjab', 'Chandigarh', 'Uttarakhand', 'Ladakh', 'Delhi', 'Haryana'],
        'Central Region': ['Uttar Pradesh', 'Madhya Pradesh', 'Chhattisgarh'],
        'Western Region': ['Rajasthan', 'Gujarat', 'Dadra and Nagar Haveli and Daman and Diu', 'Maharashtra'],
        'Eastern Region': ['Bihar', 'Jharkhand', 'Odisha', 'West Bengal', 'Sikkim'],
        'Southern Region': ['Andhra Pradesh', 'Telangana', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Puducherry', 'Goa', 'Lakshadweep', 'Andaman and Nicobar Islands'],
        'North-Eastern Region': ['Assam', 'Meghalaya', 'Manipur', 'Nagaland', 'Tripura', 'Arunachal Pradesh', 'Mizoram']
    }
    
    df['Region'] = df['State'].map({state: region for region, states in state_groups.items() for state in states})
    return df


# In[197]:


import re

# Normalize state names in lat_long_df
lat_long_df['State'] = lat_long_df['State'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x.lower()))

# Normalize state names in existing_df
map_trans_df['State'] = map_trans_df['State'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x.lower()))

# Merge the 'Latitude' and 'Longitude' columns from lat_long_df to existing_df
map_trans_df = map_trans_df.merge(lat_long_df[['State', 'District', 'Latitude', 'Longitude']], on=['State', 'District'], how='left')

# Display the merged DataFrame
map_trans_df


# In[200]:


map_trans_df = map_trans_df.drop('Longitude', axis=1)


# In[257]:


map_trans_df


# In[ ]:


##all coumns same letter and symbols


# In[203]:


lat_long_df['State'] = lat_long_df['State'].str.lower()
lat_long_df['District'] = lat_long_df['District'].str.lower()
df['State'] = df['State'].str.lower()
df['District'] = df['District'].str.lower()


# In[206]:


df_list = ['agg_trans_df',
           'agg_user_df',
           'map_trans_df',
           'map_user_df',
           'top_trans_dist_df',
           'top_user_dist_df']

df_list = [df.lower() for df in df_list]


# In[208]:


for df_name in df_list:
    df = globals()[df_name]
    df.rename(columns=lambda x: x.lower(), inplace=True)


# In[230]:


lat_long_df.columns = lat_long_df.columns.str.lower()


# In[243]:


for df_name in df_list:
    df = globals()[df_name]
    if 'latitude' in df.columns and 'longitude' in df.columns:
        df = df.drop(['latitude', 'longitude'], axis=1)
        globals()[df_name] = df


# In[ ]:


def add_region_column(df):
    state_groups = {
        'Northern Region': ['Jammu and Kashmir', 'Himachal Pradesh', 'Punjab', 'Chandigarh', 'Uttarakhand', 'Ladakh', 'Delhi', 'Haryana'],
        'Central Region': ['Uttar Pradesh', 'Madhya Pradesh', 'Chhattisgarh'],
        'Western Region': ['Rajasthan', 'Gujarat', 'Dadra and Nagar Haveli and Daman and Diu', 'Maharashtra'],
        'Eastern Region': ['Bihar', 'Jharkhand', 'Odisha', 'West Bengal', 'Sikkim'],
        'Southern Region': ['Andhra Pradesh', 'Telangana', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Puducherry', 'Goa', 'Lakshadweep', 'Andaman and Nicobar Islands'],
        'North-Eastern Region': ['Assam', 'Meghalaya', 'Manipur', 'Nagaland', 'Tripura', 'Arunachal Pradesh', 'Mizoram']
    }
    
    df['Region'] = df['State'].map({state: region for region, states in state_groups.items() for state in states})
    return df


# In[271]:


df_with_region = add_region_column(df)


# In[ ]:


#check the null values


# In[ ]:


for df_name in df_list:
    df = globals()[df_name]
    print(f"{df_name}:")
    print(f"Null count: \n{df.isnull().sum()}")
    print(f"Duplicated rows count: \n{df.duplicated().sum()}")
    


# In[ ]:


# duplicate rows


# In[ ]:


print('DATAFRAME INFO:\n')

for df_name in df_list:
    df = globals()[df_name]
    print(df_name + ':\n')
    df.info()


# In[ ]:


#outer layer of data


# In[359]:


def count_outliers(df):
    outliers = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        if col in ['Transaction_count', 'Transaction_amount']:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            upper_bound = q3 + (1.5 * iqr)
            lower_bound = q1 - (1.5 * iqr)
            outliers[col] = len(df[(df[col] > upper_bound) | (df[col] < lower_bound)])
        else:
            continue
    return outliers


# In[360]:


print('OUTLIER COUNT ACROSS DATAFRAMES:\n')

for df_name in df_list:
    df = globals()[df_name]
    outliers = count_outliers(df)
    if len(outliers) == 0:
        pass
    else:
        print(df_name, ":\n\n", outliers, "\n")
        print("\n", 55 * "_", "\n")


# In[ ]:


#mysql connection


# In[364]:


import sqlalchemy

# Establish a connection to the MySQL server
# Replace 'username', 'password', 'hostname', 'database' with your MySQL credentials
engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:12345@localhost/phonepe')

# Assuming you have a dataframe called 'df' that you want to store
table_name = 'agg_trans_df'  # Replace with the desired table name

# Store the dataframe in the MySQL database
agg_trans_df.to_sql(table_name, con=engine, if_exists='replace', index=False)


# In[365]:


import sqlalchemy

# Establish a connection to the MySQL server
# Replace 'username', 'password', 'hostname', 'database' with your MySQL credentials
engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:12345@localhost/phonepe')

# Assuming you have a dataframe called 'df' that you want to store
table_name = 'agg_user_df'  # Replace with the desired table name

# Store the dataframe in the MySQL database
agg_user_df.to_sql(table_name, con=engine, if_exists='replace', index=False)


# In[366]:


import sqlalchemy

# Establish a connection to the MySQL server
# Replace 'username', 'password', 'hostname', 'database' with your MySQL credentials
engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:12345@localhost/phonepe')

# Assuming you have a dataframe called 'df' that you want to store
table_name = 'map_trans_df'  # Replace with the desired table name

# Store the dataframe in the MySQL database
map_trans_df.to_sql(table_name, con=engine, if_exists='replace', index=False)


# In[367]:


import sqlalchemy

# Establish a connection to the MySQL server
# Replace 'username', 'password', 'hostname', 'database' with your MySQL credentials
engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:12345@localhost/phonepe')

# Assuming you have a dataframe called 'df' that you want to store
table_name = 'map_user_df'  # Replace with the desired table name

# Store the dataframe in the MySQL database
map_user_df.to_sql(table_name, con=engine, if_exists='replace', index=False)


# In[368]:


import sqlalchemy

# Establish a connection to the MySQL server
# Replace 'username', 'password', 'hostname', 'database' with your MySQL credentials
engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:12345@localhost/phonepe')

# Assuming you have a dataframe called 'df' that you want to store
table_name = 'top_trans_dist_df'  # Replace with the desired table name

# Store the dataframe in the MySQL database
top_trans_dist_df.to_sql(table_name, con=engine, if_exists='replace', index=False)


# In[369]:


import sqlalchemy

# Establish a connection to the MySQL server
# Replace 'username', 'password', 'hostname', 'database' with your MySQL credentials
engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:12345@localhost/phonepe')

# Assuming you have a dataframe called 'df' that you want to store
table_name = 'top_user_dist_df'  # Replace with the desired table name

# Store the dataframe in the MySQL database
top_user_dist_df.to_sql(table_name, con=engine, if_exists='replace', index=False)

