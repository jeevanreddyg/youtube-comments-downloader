import csv
import json
from googleapiclient.discovery import build
import logging

api_key = "YOUR_KEY"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log/logger.log"),
        logging.StreamHandler()
    ]
)

def video_comments(video_id):
    # empty list for storing reply
    replies = []

    # creating youtube resource object
    youtube = build("youtube", "v3", developerKey=api_key)
    

    # retrieve youtube video results
    video_response = (
        youtube.commentThreads()
        .list(part="snippet,replies", videoId=video_id, textFormat="plainText")
        .execute()
    )

    # iterate video response
    response_comments = []
    while True:

        for item in video_response["items"]:
            # Extracting comments
            comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            kind = item["kind"]
            videoId = item["snippet"]["videoId"]
            authorDisplayName = item["snippet"]["topLevelComment"]["snippet"][
                "authorDisplayName"
            ]
            authorProfileImageUrl = item["snippet"]["topLevelComment"]["snippet"][
                "authorProfileImageUrl"
            ]
            authorChannelUrl = item["snippet"]["topLevelComment"]["snippet"][
                "authorChannelUrl"
            ]
            likeCount = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            publishedAt = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            updatedAt = item["snippet"]["topLevelComment"]["snippet"]["updatedAt"]

            # counting number of reply of comment
            replycount = item["snippet"]["totalReplyCount"]
            comment_item = {
                "videoId": videoId,
                "coomentId": item["id"],
                "kind":kind,
                "authorDisplayName": authorDisplayName,
                "comment": comment,
                "authorProfileImageUrl": authorProfileImageUrl,
                "authorChannelUrl": authorChannelUrl,
                "likeCount": likeCount,
                "publishedAt": publishedAt,
                "updatedAt": updatedAt,
                "replycount": replycount,
                "isreply": False,
                "parentId": "NA",
            }
            response_comments.append(comment_item)

            # if reply is there
            if replycount > 0:
                
                # iterate through all reply
                for reply in item["replies"]["comments"]:
                    # Extract reply
                    reply_comment = reply["snippet"]["textDisplay"]
                    authorDisplayName = reply["snippet"]["authorDisplayName"]
                    authorProfileImageUrl = reply["snippet"]["authorProfileImageUrl"]
                    authorChannelUrl = reply["snippet"]["authorChannelUrl"]
                    likeCount = reply["snippet"]["likeCount"]
                    publishedAt = reply["snippet"]["publishedAt"]
                    updatedAt = reply["snippet"]["updatedAt"]
                    parentId = reply["snippet"]["parentId"]
                    kind = reply["kind"]
                    reply_item = {
                        "videoId": videoId,
                        "coomentId": reply["id"],
                        "kind":kind,
                        "authorDisplayName": authorDisplayName,
                        "comment": reply_comment,
                        "authorProfileImageUrl": authorProfileImageUrl,
                        "authorChannelUrl": authorChannelUrl,
                        "likeCount": likeCount,
                        "publishedAt": publishedAt,
                        "updatedAt": updatedAt,
                        "replycount": 0,
                        "isreply": True,
                        "parentId": parentId,
                    }

                    response_comments.append(reply_item)

        # Again repeat
        if "nextPageToken" in video_response:
            video_response = (
                youtube.commentThreads()
                .list(
                    part="snippet,replies",
                    videoId=video_id,
                    pageToken=video_response["nextPageToken"],
                    textFormat="plainText",
                )
                .execute()
            )
        else:
            break

    return response_comments


try:
    logging.info(f"Start running....")
    with open("input.csv", newline="") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=",")
        for row in spamreader:
            try:
                if "sno" in row[0].lower():
                    continue

                video_id = row[1]
                sno = row[0]
                
                logging.info(f"File {sno} - {video_id} start...")
                response_comments = video_comments(video_id)
                logging.info(f"File {sno} - {video_id} end...")

                logging.info(f"Save {sno} - {video_id} start...")
                
                data_file = json.dumps(response_comments, ensure_ascii=False)

                with open(f"json/{sno}_{video_id}.json", "w") as file:
                    file.write(data_file)

                keys = response_comments[0].keys()
                with open(f"csv/{sno}_{video_id}.csv", "w", newline="") as output_file:
                    dict_writer = csv.DictWriter(output_file, keys)
                    dict_writer.writeheader()
                    dict_writer.writerows(response_comments)
                    
                logging.info(f"Save {sno} - {video_id} end...")
            except Exception as ex:
                logging.error(f"Error: {ex}")
            
    logging.info(f"Completed running.")
    
except Exception as e:
    logging.error(f"Error: {e}")
