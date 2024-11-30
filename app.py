import streamlit as st
from PIL import Image
import mysql.connector
import pandas as pd
import plotly.express as px

from main import (
    create_tables,
    channel_information,
    playlist_information,
    get_video_ids,
    video_information,
    comments_information,
    insert_channel_data,
    insert_playlist_data,
    insert_video_data,
    insert_comments_data,
)

# Set page configuration
st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing",
    page_icon="youtube.png",
    layout="wide"
)

# Title and description
st.title("📊 YouTube Data Harvesting and Warehousing")
st.write("""
A Streamlit-based application to retrieve YouTube channel data using the YouTube API, store it in MySQL, 
and analyze it through a user-friendly interface.
""")


# Sidebar menu
menu = st.sidebar.selectbox(
    "Navigation",
    ["Home", "Collect and Store Data", "Database Management", "Query and Visualize Data", "Visualize the Data"]
)


# Database connection function
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Vishnu1101",
            database="youtube"
        )
        return connection
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None

if menu == "Home":
    st.header("Welcome to the YouTube Data Harvesting Tool")
    st.write("""
    This tool allows you to:
    - Retrieve data from YouTube channels, playlists, videos, and comments.
    - Store the retrieved data in a MySQL database.
    - Query and analyze the stored data.
    """)

    
    st.markdown('''Hi there! I'm Nesal Prabhu M, an M.Sc graduate passionate about data science and analytics 📊. 
                I'm diving into the world of data science, and my first project is all about YouTube Data Harvesting and Warehousing using SQL 🎥💻. 
                In this project, I explored YouTube data to extract valuable insights 🔍. 
                It’s been an exciting experience that fueled my interest in data-driven decisions 
                and helped me improve my skills in data extraction and database management 💡📈 techniques and Database management.''')
    st.subheader(':blue[Contact:]')
    st.markdown('#### linkedin: https://www.linkedin.com/in/nesal-prabhu-a80a01278/')
    st.markdown('#### Email : nesalprabhu31@gmail.com')

elif menu == "Collect and Store Data":
    st.header("Collect and Store Data")

    # Input for channel ID
    channel_id = st.text_input("Enter YouTube Channel ID", "")

    if st.button("Collect and Store Data"):
        if channel_id:
            try:
                # Insert channel data
                channel_data = channel_information(channel_id)
                insert_channel_data(channel_data)
                st.success(f"Channel '{channel_data['channel_name']}' data inserted successfully.")

                # Insert playlist data
                playlist_data = playlist_information(channel_id)
                insert_playlist_data(playlist_data)
                st.success("Playlist data inserted successfully.")

                # Insert video data
                video_ids = get_video_ids(channel_id)
                video_data = video_information(video_ids)
                insert_video_data(video_data)
                st.success("Video data inserted successfully.")

                # Insert comments data
                comments_data = comments_information(video_ids)
                insert_comments_data(comments_data)
                st.success("Comments data inserted successfully.")

            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.error("Please enter a valid YouTube Channel ID.")

elif menu == "Database Management":
    st.header("Database Management")
    if st.button("Create Tables"):
        try:
            create_tables()
            st.success("Database initialized and tables created successfully!")
        except Exception as e:
            st.error(f"Error: {e}")

elif menu == "Query and Visualize Data":
    st.header("Query and Visualize Data")
    connection = connect_to_mysql()
    if connection:
        cursor = connection.cursor(dictionary=True)

        # Query options
        query_options = [
            "Top 10 Most Viewed Videos",
            "Channel with Most Subscribers",
            "Top 10 Videos by Likes",
            "Average Views per Video",
            "Videos Count per Channel",
            "Top 5 Most Commented Videos",
            "Total Number of Comments",
            "Playlists with Most Videos",
            "Total Videos Across All Channels",
            "Newest Videos by Publish Date"
        ]
        selected_query = st.selectbox("Select a Query", query_options)

        # Query mapping
        query_mapping = {
            "Top 10 Most Viewed Videos": "SELECT video_name, view_count FROM videos ORDER BY view_count DESC LIMIT 10;",
            "Channel with Most Subscribers": "SELECT channel_name, channel_subscribers FROM channels ORDER BY channel_subscribers DESC LIMIT 1;",
            "Top 10 Videos by Likes": "SELECT video_name, like_count FROM videos ORDER BY like_count DESC LIMIT 10;",
            "Average Views per Video": "SELECT AVG(view_count) AS average_views FROM videos;",
            "Videos Count per Channel": "SELECT channel_name, COUNT(video_id) AS video_count FROM videos GROUP BY channel_name;",
            "Top 5 Most Commented Videos": "SELECT video_name, comment_count FROM videos ORDER BY comment_count DESC LIMIT 5;",
            "Total Number of Comments": "SELECT SUM(comment_count) AS total_comments FROM videos;",
            "Playlists with Most Videos": "SELECT playlist_name, videoscount FROM playlist ORDER BY videoscount DESC LIMIT 5;",
            "Total Videos Across All Channels": "SELECT SUM(channel_video_count) AS total_videos FROM channels;",
            "Newest Videos by Publish Date": "SELECT video_name, publishedat FROM videos ORDER BY publishedat DESC LIMIT 10;"
        }

        if st.button("Run Query"):
            query = query_mapping[selected_query]
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                st.write(f"Results for: {selected_query}")
                st.dataframe(results)
            except Exception as e:
                st.error(f"Error executing query: {e}")
    else:
        st.error("Failed to connect to the database.")

elif menu == "Visualize the Data":
    st.header("Visualize the Data")
    connection = connect_to_mysql()
    if connection:
        cursor = connection.cursor(dictionary=True)

        # Example: Top 10 most viewed videos
        cursor.execute("SELECT video_name, view_count FROM videos ORDER BY view_count DESC LIMIT 10;")
        results = cursor.fetchall()
        df = pd.DataFrame(results)

        # Bar Chart for Most Viewed Videos
        if not df.empty:
            fig = px.bar(df, x="video_name", y="view_count", title="Top 10 Most Viewed Videos")
            st.plotly_chart(fig)

        # Pie Chart for Video Counts by Channel
        cursor.execute("SELECT channel_name, COUNT(video_id) AS video_count FROM videos GROUP BY channel_name;")
        results = cursor.fetchall()
        df_channel = pd.DataFrame(results)

        if not df_channel.empty:
            fig = px.pie(df_channel, names="channel_name", values="video_count", title="Video Count by Channel")
            st.plotly_chart(fig)
    else:
        st.error("Failed to connect to the database.")
