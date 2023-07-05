import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px

# Establish a connection to MySQL
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345',
    database='phonepe'
)

# Function to fetch data from the selected table
def fetch_table_data(selected_table):
    query = f"SELECT * FROM {selected_table}"
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data, columns=[column[0] for column in cursor.description])

    return df

# Function to create a pie chart
def create_pie_chart(df, values_col, names_col):
    fig = px.pie(df, values=values_col, names=names_col)
    st.plotly_chart(fig)

def create_bar_chart(df, x_col, y_col):
    fig = px.bar(df, x=x_col, y=y_col)
    st.plotly_chart(fig)

# Function to create a map visualization using scatter_geo
def create_map_visualization(df):
    fig = px.scatter_geo(df, lat="latitude", lon="longitude", hover_name="district", color="district",projection="natural earth")
    st.plotly_chart(fig)

# Create the Streamlit app
def main():
    st.title("phone infromation tables")
   

    # List of table names
    table_names = ['agg_trans_df', 'agg_user_df', 'map_trans_df', 'map_user_df', 'top_trans_dist_df', 'top_user_dist_df']

    # Dropdown select box for table names
    selected_table = st.selectbox("Select a table", table_names)

    # Check if a table is selected
    if selected_table:
        # Fetch data from the selected table
        df = fetch_table_data(selected_table)

        # Display the data table
        st.subheader("Data Table")
        st.dataframe(df)

        # Perform visualization based on the selected table
        if selected_table == 'agg_trans_df':
            # Check if the transaction_count and transaction_type columns exist
            if 'transaction_count' in df.columns and 'transaction_type' in df.columns:
                # Create the pie chart
                st.subheader("Pie Chart")
                create_pie_chart(df, 'transaction_count', 'transaction_type')
            else:
                st.write("Selected table does not contain the required columns for the pie chart.")
        
        elif selected_table == 'agg_user_df':
            # Check if the registered_users and district columns exist
            if 'registered_users' in df.columns and 'district' in df.columns:
                # Create the pie chart
                st.subheader("Pie Chart")
                create_pie_chart(df, 'registered_users', 'district')
            else:
                st.write("Selected table does not contain the required columns for the pie chart.")
        
        elif selected_table == 'map_trans_df':
            # Check if the required columns exist for the map visualization
            if 'latitude' in df.columns and 'longitude' in df.columns:
                # Create the map visualization
                st.subheader("Map Visualization")
                create_map_visualization(df)

                # Check if the 'district' and 'transaction count' columns exist
                if 'district' in df.columns and 'transaction count' in df.columns:
                    # Create the bar chart
                    st.subheader("Bar Chart")
                    create_bar_chart(df, 'district', 'transaction count')
                else:
                    st.write("Selected table does not contain the required columns for the bar chart.")
            else:
                st.write("Selected table does not contain the required columns for the map visualization.")


        elif selected_table == 'map_user_df':
            # Check if the latitude, longitude, and registered_users columns exist
            if 'latitude' in df.columns and 'longitude' in df.columns and 'registered_users' in df.columns:
                # Create the map visualization using scatter_geo
                st.subheader("Map Visualization")
                create_map_visualization(df)
                
                # Check if the 'district' and 'registered_users' columns exist
                if 'district' in df.columns and 'registered_users' in df.columns:
                    # Create the bar chart
                    st.subheader("Bar Chart")
                    create_bar_chart(df, 'district', 'registered_users')
                else:
                    st.write("Selected table does not contain the required columns for the bar chart.")
            else:
                st.write("Selected table does not contain the required columns for the map visualization.")

        elif selected_table == 'top_trans_dist_df':
            # Check if the transaction_count, district, latitude, and longitude columns exist
            if 'transaction_count' in df.columns and 'district' in df.columns and 'latitude' in df.columns and 'longitude' in df.columns:
                # Create the pie chart
                st.subheader("Pie Chart")
                create_pie_chart(df, 'transaction_count', 'district')
                
                # Create the map visualization using scatter_geo
                st.subheader("Map Visualization")
                create_map_visualization(df)
            else:
                st.write("Selected table does not contain the required columns for the pie chart and map visualization.")

        elif selected_table == 'top_user_dist_df':
            # Check if the registered_users, district, latitude, and longitude columns exist
            if 'registered_users' in df.columns and 'district' in df.columns and 'latitude' in df.columns and 'longitude' in df.columns:
                # Create the pie chart
                st.subheader("Pie Chart")
                create_pie_chart(df, 'registered_users', 'district')

                # Create the map visualization using scatter_geo
                st.subheader("Map Visualization")
                create_map_visualization(df)
            else:
                st.write("Selected table does not contain the required columns for the pie chart and map visualization.")
            
if __name__ == "__main__":
    main()
