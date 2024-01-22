from googleapiclient.discovery import build
from pprint import pprint
import pymongo
from pymongo import MongoClient
import pandas as pd
import pymysql
import mysql.connector
import numpy as np
import streamlit as st
api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyBrb_UOgdI8kj4Eo0rCQznOneT2v4IGRzg'
Channel_id = 'UC5Ddbrr6sH39HWZhZ0tjrSQ'
youtube = build(api_service_name, api_version, developerKey=api_key)
client=pymongo.MongoClient("mongodb://localhost:27017")
mydb=client["e"]
mycol=mydb["Youtubedatacol"]
from pymongo import MongoClient
client=pymongo.MongoClient("mongodb://localhost:27017")
mydb=client["e"]
mycol=mydb["Youtubedatacol"]
# 10 Channel ids
# 1. Mic_Set = UC5EQWvy59VeHPJz8mDALPxg
# 2. Village_cooking = UCk3JZr7eS3pg5AGEvBdEvFg
# 3. Boo_Tech = UCI9-3sdWwj9UJnFlNC01HVQ
# 4. Kashmir Vendor = UCeoJf9nHDJDyMlyF6Oh_lJQ
# 5. Art Blogger = UCdD0917TE7xdN2fb-GFlBeQ
# 6. Mysterious_Facts = UC3bg35LPPe0yKiPu7MNwRPg
# 7. Sriyesh Vlogs = UCtMKV43qwEDYlJ-ttiK6Rag
# 8. Fun_Science = UCTn0kzSoLyz6r6H-psXmgjA
# 9. Mahasvin = UCm6jDZ9yHhRL8be2-R9XWZQ
# 10 abi kumaran = UC5Ddbrr6sH39HWZhZ0tjrSQ
request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=Channel_id
)
response = request.execute()
pprint(response)
def channel_details(Channel_id):
    api_service_name = "youtube"
    api_version = "v3"
    api_key='AIzaSyBrb_UOgdI8kj4Eo0rCQznOneT2v4IGRzg'
    youtube = build(api_service_name, api_version, developerKey=api_key)
    # Channel_id = 'UC5Ddbrr6sH39HWZhZ0tjrSQ'

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=Channel_id
    )
    response = request.execute()
    ChDetails=dict(Channel_name = response['items'][0]['snippet']['title'],
                   Channel_desc = response['items'][0]['snippet']['description'],
                   Channel_published = response['items'][0]['snippet']['publishedAt'],
                    Channel_playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    Channel_subscriberCount = response['items'][0]['statistics']['subscriberCount'],
                    Channel_videoCount = response['items'][0]['statistics']['videoCount'],
                    Channel_viewCount = response['items'][0]['statistics']['viewCount'])
    return ChDetails
channelResult = channel_details(Channel_id)
# pprint(channelResult)
def playlist_details(Channel_id):
    token=None
    video_ids_list=[]
    request=youtube.channels().list(
        part="ContentDetails",
        id=Channel_id
    )
    response=request.execute()
    for i in response ['items']:
        playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
    while True:
        request2 = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=token
            
            )
        response2 = request2.execute()
        
        for item in response2["items"]:
            video_id=item["contentDetails"]["videoId"]
            video_ids_list.append(video_id)

        if 'nextPageToken' in response2:
            token = response2.get('nextPageToken')
        else:
            break
    return video_ids_list
playlistResult = playlist_details(Channel_id)
# print(len(playlistResult))
# pprint(playlistResult)
def video_details(playlistResult):
    videos_list=[]
    for i in playlistResult:
        request3 = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i,
        )
        response3=request3.execute()
        videos_info={"channelId":response3["items"][0]["snippet"]["channelId"],
                     "channelTitle":response3["items"][0]["snippet"]["channelTitle"],
                     "video_id": response3["items"][0]["id"],
                     "video_title":response3["items"][0]["snippet"]["title"],
                     "Duration":response3["items"][0]["contentDetails"]["duration"],
                     "video_desc":response3["items"][0]["snippet"]["description"],
                     "publishedat":response3["items"][0]["snippet"]["publishedAt"],
                     "thumbnails":response3["items"][0]["snippet"]["thumbnails"]["default"]["url"],
                     "commentCount":response3["items"][0]["statistics"]["commentCount"],
                     "favouriteCount":response3["items"][0]["statistics"]["favoriteCount"],
                     "likeCount":response3["items"][0]["statistics"]["likeCount"],
                     "viewCount":response3["items"][0]["statistics"]["viewCount"],
                    }
        videos_list.append(videos_info)
    return videos_list
videoResult = video_details(playlistResult)
# print(len(videoResult))
# pprint(videoResult)
youtube = build(api_service_name, api_version, developerKey=api_key)

request4 = youtube.comments().list(
    part="snippet",
    parentId="UgzDE2tasfmrYLyNkGt4AaABAg"
)
response4 = request4.execute()

pprint(response4)
# from pprint import pprint
pprint(len(response['items']))
pprint(response)
Comment_name = response4['items'][0]['snippet']['authorDisplayName']
like_count = response4['items'][0]['snippet']['likeCount']
comment_details = response4['items'][0]['snippet']['textDisplay']
comment_updatedAt = response4['items'][0]['snippet']['updatedAt']
comment_channel_id = response4['items'][0]['snippet']['channelId']
comment_rating = response4['items'][0]['snippet']['viewerRating']
d = {
    'comment': Comment_name,
    'likeCount': like_count,
    'commentDetails': comment_details,
    'commentUpdatedAt': comment_updatedAt,
    'CommentChannelid': comment_channel_id,
    'commentRating': comment_rating
}
def comment_details(playlistResult):

    comments_list=[]
    for i in playlistResult:
        try:
            request4 = youtube.commentThreads().list(
                part="snippet",
                videoId=i,
                maxResults=20
            )
            response4 = request4.execute()
            for i in range(0,len(response4["items"])):
                comment_details = dict(Comment_id = response4['items'][i]['snippet']['topLevelComment']['id'],
                                       video_id = response4['items'][i]['snippet']['videoId'],
                                       comment_text = response4['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                                       comment_publishedAt = response4['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                                       comment_authorname = response4['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'])

                comments_list.append(comment_details)
        except:
            pass
    return comments_list
commentsResult = comment_details(playlistResult)
# print(len(commentsResult))
# pprint (commentsResult)
def main(Channel_id):
    # Channel_id = 'UC5Ddbrr6sH39HWZhZ0tjrSQ'
    channelResult = channel_details(Channel_id)
    playlistResult = playlist_details(Channel_id)
    videoResult = video_details(playlistResult)
    commentsResult = comment_details(playlistResult)
    data = {"channel_details":channelResult,
            "PlaylistDetails":playlistResult,
            "VideoDetails":videoResult,
            "CommentDetails":commentsResult}
    return data
finalResult = main(Channel_id)
# pprint(finalResult)
# !pip install pymongo
def mongocode(Channel_id):
    client=pymongo.MongoClient("mongodb://localhost:27017")
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]
    from pymongo import MongoClient
    client=pymongo.MongoClient("mongodb://localhost:27017")
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]
    
    channeldata=channelResult
    playlistdata=playlistResult
    videodata=videoResult
    commentdata=commentsResult
    channel={
        "ChannelData":channeldata,
        "PlaylistData":playlistdata,
        "VideoData":videodata,
        "CommentData":commentdata
    }
    mycol.insert_one(channel)
# !pip install pymysql
# !pip install mysql-connector-python

myconnection=pymysql.connect(host="localhost",user="root",password="Password@1234",database='Youtubedataharvesting')
cur=myconnection.cursor()
def Channels_table():
    drop_query='''DROP TABLE IF EXISTS channel_details'''
    cur.execute(drop_query)
    # myconnection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS channel_details ( Channel_desc text,
                                                                   Channel_name varchar(255) primary key,
                                                                   Channel_playlist varchar(255),
                                                                   Channel_published varchar(255),
                                                                   Channel_subscriberCount bigint,
                                                                   Channel_videoCount int, 
                                                                   Channel_viewCount bigint)'''
    cur.execute(create_query)
    # myconnection.commit()
    ch_list = []
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]

    for c in mycol.find({}, {'_id': 0, 'ChannelData': 1}):
        ch_list.append(c['ChannelData'])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channel_details(  Channel_desc,
                                                        Channel_name,
                                                        Channel_playlist,
                                                        Channel_published,
                                                        Channel_subscriberCount,
                                                        Channel_videoCount,
                                                        Channel_viewCount)
                                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_desc'],
                  row['Channel_name'],
                  row['Channel_playlist'],
                  row['Channel_published'].replace("T","").replace("Z",""),
                  row['Channel_subscriberCount'],
                  row['Channel_videoCount'],
                  row['Channel_viewCount'])
#         cur.execute(insert_query, values)
#         myconnection.commit()
        try:
            cur.execute(insert_query, values)
            myconnection.commit()
        except:
            print("channels values are already inserted")    
# Channels_table()
def videos_table():
    drop_query='''DROP TABLE IF EXISTS video_details'''
    cur.execute(drop_query)
#     myconnection.commit()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS video_details ( channelId varchar(255),
                                                                 channelTitle varchar(255),
                                                                 video_id varchar(255),
                                                                 video_title varchar(255),
                                                                 viewCount int,
                                                                 favouriteCount int,
                                                                 commentCount int,
                                                                 likeCount int, 
                                                                 Duration varchar(255),
                                                                 publishedat varchar(255))'''
        cur.execute(create_query)
        myconnection.commit()
    except:
        print("video table already created") 
    vi_list = []
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]

    for v in mycol.find({}, {'_id': 0, 'VideoData': 1}):
        for i in range(len(v['VideoData'])):
            vi_list.append(v['VideoData'][i])

    df1 = pd.DataFrame(vi_list)
    
    for index, row in df1.iterrows():
        insert_query = '''INSERT INTO video_details(channelId,
                                                    channelTitle,
                                                    video_id,
                                                    video_title,
                                                    viewCount,
                                                    favouriteCount,
                                                    commentCount,
                                                    likeCount,
                                                    Duration,
                                                    publishedat)
                                                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['channelId'],
                  row['channelTitle'],
                  row['video_id'],
                  row['video_title'],
                  row['viewCount'],
                  row['favouriteCount'],
                  row['commentCount'],
                  row['likeCount'],
                  row['Duration'].replace('PT', '').replace('H', ':').replace('M', ':').split('S')[0],
                  row['publishedat'].replace("T","").replace("Z",""))
        cur.execute(insert_query, values)
        myconnection.commit()             
# videos_table()
def comments_table():
    drop_query='''DROP TABLE IF EXISTS comment_details'''
    cur.execute(drop_query)
#     myconnection.commit()
    try:        
        create_query = '''CREATE TABLE IF NOT EXISTS comment_details (Comment_id varchar(255) primary key,
                                                                   video_id varchar(255),
                                                                   comment_authorname varchar(255),
                                                                   comment_publishedAt varchar(255),
                                                                   comment_text text)'''
        cur.execute(create_query)
        myconnection.commit()
    except:
        print("comments table already created")
    comm_list = []
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]

    for comm in mycol.find({}, {'_id': 0, 'CommentData': 1}):
            for i in range(len(comm['CommentData'])):
                comm_list.append(comm['CommentData'][i])

    df2 = pd.DataFrame(comm_list)

    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO comment_details(Comment_id,
                                                        video_id,
                                                        comment_authorname,
                                                        comment_publishedAt,
                                                        comment_text)
                                                        VALUES(%s,%s,%s,%s,%s)'''
        values = (row['Comment_id'],
                  row['video_id'],
                  row['comment_authorname'],
                  row['comment_publishedAt'].replace("T","").replace("Z",""),
                  row['comment_text'])
        cur.execute(insert_query, values)
        myconnection.commit()
# comments_table()
def Tables():
    Channels_table()
    videos_table()
    comments_table()
    return "Tables created successfully"

with st.sidebar:   

    st.header("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.caption("MongoDB")
    st.caption("python scripting")
    st.caption("data collection")
    st.caption("data management using MongoDb and sql")
    st.caption("API INTEGRATION")
channel_id=st.text_input("ENTER CHANNEL ID")
if st.button("Collect and store data"):
    ch_list = []
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]
    for c in mycol.find({}, {'_id': 0, 'ChannelData': 1}):
        ch_list.append(c['ChannelData']['Channel_name'])
    if channel_id in ch_list:
        st.success("channel details of the given channel id already exits")
    else:
        insert=main(channel_id)
        st.success(insert)
if st.button("Migrate to SQL"):
    table=Tables()     
    st.success(table)
myconnection=pymysql.connect(host="localhost",user="root",password="Password@1234",database='Youtubedataharvesting')
cur=myconnection.cursor()       
def show_Channels_table():
    ch_list = []
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]

    for c in mycol.find({}, {'_id': 0, 'ChannelData': 1}):
        ch_list.append(c['ChannelData'])

    df = pd.DataFrame(ch_list)
    df
def show_videos_table():
    vi_list = []
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]

    for v in mycol.find({}, {'_id': 0, 'VideoData': 1}):
        for i in range(len(v['VideoData'])):
            vi_list.append(v['VideoData'][i])

    df1 = pd.DataFrame(vi_list)
    df1
def show_comments_table():
    comm_list = []
    mydb=client["e"]
    mycol=mydb["Youtubedatacol"]

    for comm in mycol.find({}, {'_id': 0, 'CommentData': 1}):
            for i in range(len(comm['CommentData'])):
                comm_list.append(comm['CommentData'][i])

    df2 = pd.DataFrame(comm_list)
    df2
#showing table in streamlit
show_table=st.radio("SELECT THE TABLE FOR VIEW",("channels","video","comment"))

if show_table =="channels" :
    show_Channels_table()
elif show_table =="video" :
    show_videos_table()
elif show_table =="comment" :
    show_comments_table()    
#sql connection
myconnection=pymysql.connect(host="localhost",user="root",password="Password@1234",database='Youtubedataharvesting')
cur=myconnection.cursor()

#executing query

question=st.selectbox("SELECT YOUR QUESTION",("1.What are the names of all the videos and their corresponding channels?",
                    "2.Which channels have the most number of videos, and how many videos do they have?",
                    "3.What are the top 10 most viewed videos and their respective channels?",
                    "4.How many comments were made on each video, and what are their corresponding video names?",
                    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                    "8.What are the names of all the channels that have published videos in the year 2022?",
                    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
))
if question=="1.What are the names of all the videos and their corresponding channels?":

    query1='''select video_title as videos,channelTitle as channelname from video_details'''
    cur.execute(query1)
    myconnection.commit()
    t1=cur.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)
elif question=="2.Which channels have the most number of videos, and how many videos do they have?":

    query2='''select Channel_name as channelname,Channel_videoCount as no_videos from channel_details
                order by Channel_videoCount desc'''
    cur.execute(query2)
    myconnection.commit()
    t2=cur.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)
elif question=="3.What are the top 10 most viewed videos and their respective channels?":

    query3='''select viewCount as views,channelTitle as channelname,video_title as videotitle from video_details
                where viewCount is not null order by views desc limit 10'''
    cur.execute(query3)
    myconnection.commit()
    t3=cur.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","video title"])
    st.write(df3)
elif question=="4.How many comments were made on each video, and what are their corresponding video names?":

    query4='''select commentCount as no_comments,video_title as videotitle from video_details where commentCount is not null'''
    cur.execute(query4)
    myconnection.commit()
    t4=cur.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comments","videotitle"])
    st.write(df4)
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":

    query5='''select video_title as videotitle,channelTitle as channelname,likeCount as likescount
            from video_details where likeCount is not null order by likeCount desc'''
    cur.execute(query5)
    myconnection.commit()
    t5=cur.fetchall()
    df5=pd.DataFrame(t5,columns=["video title","channel name","likes"])

    st.write(df5)
elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":

    query6='''select likeCount as likecount,video_title as videotitle from video_details'''
    cur.execute(query6)
    myconnection.commit()
    t6=cur.fetchall()
    df6=pd.DataFrame(t6,columns=["likes","video title"])

    st.write(df6)
elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":

    query7='''select Channel_viewCount as totalviews, Channel_name as channelname from channel_details'''
    cur.execute(query7)
    myconnection.commit()
    t7=cur.fetchall()
    df7=pd.DataFrame(t7,columns=["views","channel name"])

    st.write(df7) 
elif question=="8.What are the names of all the channels that have published videos in the year 2022?":

    query8='''select video_title as video_title,publishedat as publisheddate,channelTitle as channelname from video_details
            where publishedat=2022;'''
    cur.execute(query8)
    myconnection.commit()
    t8=cur.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","published date","channel_name"])
    
    st.write(df8)
elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    query9='''select channelTitle as channelname,AVG(Duration) as averageduration
            from video_details group by channelTitle'''
    cur.execute(query9)
    myconnection.commit()
    t9=cur.fetchall()
    df9=pd.DataFrame(t9,columns=["channel name","avg Duration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channel name"]
        average_duration=row["avg Duration"]
        averageduration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=averageduration_str))
    df11=pd.DataFrame(T9)    
    st.write(df11)
elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":

    query10='''select video_title as videotitle,commentCount as comments,channelTitle as channelname
            from video_details where commentCount is not null order by commentCount desc'''
    cur.execute(query10)
    myconnection.commit()
    t10=cur.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","comments","channel_name"])
    st.write(df10)  
