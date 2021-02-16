import os
import sys
import re
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage
)
from linebot.exceptions import (
    LineBotApiError, InvalidSignatureError
)
import logging
import json
import boto3

# スクレイピング用 追加ライブラリ

import pandas as pd
from bs4 import BeautifulSoup as bs
import urllib.request as req


logger = logging.getLogger()
logger.setLevel(logging.INFO)

update_now = False

channel_secret = os.getenv('LINE_CHANNEL_SECRET', None)
channel_access_token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN', None)
if channel_secret is None:
    logger.error('Specify LINE_CHANNEL_SECRET as environment variable.')
    sys.exit(1)
if channel_access_token is None:
    logger.error('Specify LINE_CHANNEL_ACCESS_TOKEN as environment variable.')
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)

tatum_dict = {
    "てーたむ": "Jayson Tatum", "るか": "Luka Dončić", "やにす": "Giannis Antetokounmpo", "れぶろん": "LeBron James", "らめろ": "LaMelo Ball",
    "けんば": "Kemba Walker", "かりー": "Stephen Curry", "よきっち": "Nikola Jokić", "ヘジテーション": "Jaylen Brown", "るい": "Rui Hachimura", "ゆうた": "Yuta Watanabe",
    "kp": "Kristaps Porziņģis", "めろ": "Carmelo Anthony", "ぺいとん": "Payton Pritchard", "ぶっかー": "Devin Booker", "ad": "Anthony Davis"
}

def lambda_handler(event, context):
    
    logger.info(json.dumps(event))
    
    dynamoDB = boto3.resource("dynamodb")
    table = dynamoDB.Table(os.environ['DB'])
    
    if 'event_name' in event:
        if event['event_name'] == 'scrape':
    
            # 定期実行
            players, stats = scrape()
            
            for p,s in zip(players,stats):
                item = {"player": p, "stats": s}
                table.put_item(Item=item)
            
    else:
        # LINE返信用
        if "x-line-signature" in event["headers"]:
            signature = event["headers"]["x-line-signature"]
        elif "X-Line-Signature" in event["headers"]:
            signature = event["headers"]["X-Line-Signature"]
        body = event["body"]
        ok_json = {"isBase64Encoded": False,
                  "statusCode": 200,
                  "headers": {},
                  "body": ""}
        error_json = {"isBase64Encoded": False,
                      "statusCode": 500,
                      "headers": {},
                      "body": "Error"}
    
            
        @handler.add(MessageEvent, message=TextMessage)
        def message(line_event):
            text = line_event.message.text
            # text = make_message(table, text)
            flex_message_json_dict = make_message(table, text)
            
            if update_now:
                line_bot_api.reply_message(line_event.reply_token, TextSendMessage(text='Sorry, We Update now.'))
            else:
                line_bot_api.reply_message(line_event.reply_token, FlexSendMessage(alt_text='alt_text', contents=flex_message_json_dict))
            
        try:
            handler.handle(body, signature)
        except LineBotApiError as e:
            logger.error("Got exception from LINE Messaging API: %s\n" % e.message)
            for m in e.error.details:
                logger.error("  %s: %s" % (m.property, m.message))
            return error_json
        except InvalidSignatureError:
            return error_json

        return ok_json


    
def table_scan(table, text):
    ## line msg => Player name => DynamoDBから探す => (player, stats)を返す
    response = table.scan()
    items = response["Items"]
    player_list = [item['player'] for item in items]
    text = text.lower()
    for player in player_list:
        if re.search(text, player.lower()):
            Player = player
            break
    else:
        return None, None
        
    
    item = table.get_item(Key={"player": Player})
    player = item["Item"]["player"]
    stats = item["Item"]["stats"]
    
    return player, stats
    
        
def scrape():
    url = r"https://www.basketball-reference.com/boxscores/"
    res = req.urlopen(url)
    soup = bs(res, "lxml")
    
    today_stats = soup.find_all("div", {"class":"game_summary expanded nohover"})
    
    return_df = pd.DataFrame()
    
    for today_stat in today_stats:
        link = today_stat.find_all('a')[1]['href']
        game_url = url + os.path.basename(link)
        game_url_id = os.path.splitext(os.path.basename(link))[0]
        # 個々の試合のurlをもとにもう一度html.parser
        game_res = req.urlopen(game_url)
        game_soup = bs(game_res, "html.parser")
        tables = game_soup.find_all(id=re.compile("^box-[A-Z]{3}-game-basic$"))
        
        team_li = [tables[i].attrs["id"].split('-')[1] for i in range(2)]
        
        df = pd.DataFrame()
        
        for i, table in enumerate(tables):
            # score
            rows = table.findAll('tr')
            one_table = []
            col_row=[]
            for row_num, row in enumerate(rows):
                if row_num == 0:
                    continue
                elif row_num == 1:
                    for cell in row.findAll(['td', 'th']):
                        col_row.append(cell.get_text())
                    col_row.append('img_id')
                else:
                    one_row = []
                    row_head = row.find('th')
                    player_name = row_head.get_text()
                    if player_name == 'Team Totals' or player_name == 'Reserves':continue
                    one_row.append(player_name)
                    img_id = row_head.attrs['data-append-csv']
                    for cell in row.findAll(['td']):
                        text = cell.get_text()
                        if text == 'Did Not Play' or text == 'Did Not Dress':
                            one_row += ['-'] * 20
                        else:
                            one_row.append(text)
                    one_row.append(img_id)
                    one_table.append(one_row)
            
            one_df = pd.DataFrame(one_table, columns=col_row)
            
            one_df["team"] = team_li[i]
            one_df["opp"] = team_li[::-1][i]
            
            
            df = pd.concat([df, one_df])
            df["day"] = str(game_url)[-13:-9]
            df["day2"] = game_soup.find_all('div', {'class':'scorebox_meta'})[0].findAll('div')[0].text.replace(",", "-")
            df["place"] = game_soup.find_all('div', {'class':'scorebox_meta'})[0].findAll('div')[1].text.replace(",", "-")
            df["game_url_id"] = game_url_id

        return_df = pd.concat([return_df, df])
        
    
    # return_dfの整形
    return_df = return_df[["Starters", "MP", "3P", "3PA", "3P%", "TRB", "AST", "STL", "BLK", "PTS", "day", "team", "opp", "day2", "place","img_id", "game_url_id"]].reset_index(drop=True)
    return_df.rename(columns={"Starters": "player"}, inplace=True)
    return_df.set_index("player", drop=True, inplace=True)
    
    # スタッツを一つの列にまとめる。
    return_df["stats"]= return_df.apply(lambda x: ",".join(map(str,x)), axis=1)

    dic = return_df["stats"].to_dict()
    
    return dic.keys(), dic.values()

def make_message(table, text):
    
    if text in tatum_dict:
        text = tatum_dict[text]
        
    player, stats = table_scan(table, text)
    
    if player and stats:
        mp, three_p, three_pa, three_pp, trb, ast, stl, blk, pts, day, team, opp, day2, place, img_id, game_url_id = stats.split(",")
        
        img_url = f"https://www.basketball-reference.com/req/0/images/players/{img_id}.jpg"
        team_icon_url = f"https://d2p3bygnnzw9w3.cloudfront.net/req/202102091/tlogo/bbr/{team}-2021.png"
        box_score_url = f"https://www.basketball-reference.com/boxscores/{game_url_id}.html"
        reference_url = r"https://www.basketball-reference.com/boxscores/"
    else:
        flex_error_json_dict = {
            "type": "bubble",
            "body":{
                "type": "box",
                "layout": "horizontal",
                "contents" :[
                        {
                            "type": "text",
                            "text": "error"
                        }
                    ]
            }
                
        }
        return flex_error_json_dict
    
    flex_message_json_dict = {
        "type": "bubble",
        "hero":
            {
            "type": "image",
            "url": img_url,
            "size": "4xl",
            "aspectMode": "fit",
            "backgroundColor": "#ffffff",
            # "gravity": "bottom",
            # "align": "start",
            "action":
                {
                    "type": "uri",
                    "uri": box_score_url
                }

            },
        "body":
            {
            "type": "box",
            "layout": "vertical",
            "contents":
                [
                    {
                        "type": "text",
                        "text": player,
                        "weight": "bold",
                        "size": "xl",
                        "style": "italic",
                        # "decoration": "underline",
                        "position": "relative",
                        "align": "center",
                        "gravity": "center"
                    },
                    {
                        "type": "separator"
                    },
                    {
                        "type": "box",
                        "layout": "baseline",
                        "margin": "md",
                        "contents": [
                        {
                            "type": "icon",
                            "size": "3xl",
                            "url": team_icon_url
                        },
                        {
                            "type": "text",
                            "text": team,
                            "size": "md",
                            "color": "#999999",
                            "margin": "md",
                            "flex": 0
                        }
                        ]
                    },
                    {
                        "type": "box",
                        "layout": "vertical",
                        "margin": "lg",
                        "spacing": "sm",
                        "contents": [
                            {
                                "type": "box",
                                "layout": "baseline",
                                "spacing": "sm",
                                "contents": [
                                    {
                                        "type": "text",
                                        "text": "Place",
                                        "color": "#aaaaaa",
                                        "size": "sm",
                                        "flex": 1
                                    },
                                    {
                                        "type": "text",
                                        "text": place,
                                        "wrap": True,
                                        "color": "#666666",
                                        "size": "sm",
                                        "flex": 5
                                    }
                                ]
                            },
                            {
                                "type": "box",
                                "layout": "baseline",
                                "spacing": "sm",
                                "contents": [
                                    {
                                        "type": "text",
                                        "text": "Time",
                                        "color": "#aaaaaa",
                                        "size": "sm",
                                        "flex": 1
                                    },
                                    {
                                        "type": "text",
                                        "text": day2,
                                        "color": "#666666",
                                        "size": "sm",
                                        "flex": 5
                                    }
                                ]
                            },
                            {
                                "type": "box",
                                "layout": "baseline",
                                "spacing": "sm",
                                "contents": [
                                    {
                                        "type": "text",
                                        "text": "Stats",
                                        "color": "#aaaaaa",
                                        "size": "sm",
                                        "flex": 1
                                    },
                                    {
                                        "type": "text",
                                        "text": "SORRY, UPDATE NOW.",
                                        "color": "#aaaaaa",
                                        "size": "xs",
                                        "flex": 5
                                    }
                                ]
                            },
                        ]
                    }
                ]
            },
        "footer":
            {
                "type": "box",
                "layout": "vertical",
                "flex": 0,
                # "backgroundColor": "#000000",
                "spacing": "sm",
                "contents": [
                    {
                        "type": "button",
                        "style": "secondary",
                        "height": "sm",
                        "action": {
                            "type": "uri",
                            "label": "GAME DETAIL ↗️",
                            "uri": box_score_url
                        }
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "height": "sm",
                        "action":{
                            "type": "uri",
                            "label": "BASKETBALL REFERENCE ↗",
                            "uri": reference_url
                        }
                    }
                ]
            }
        # "styles": {
        #     "header": {
        #         "backgroundColor": "#5f9ea0"
        #     }
        # }
    }
    return flex_message_json_dict
