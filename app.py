import operator
import string
import random
import datetime
import uuid

from flask import Flask, render_template, request, session, redirect, url_for,make_response
import requests
import io
import json
import http.client
import pandas as pd
from sklearn.metrics import brier_score_loss
from bs4 import BeautifulSoup
import numpy as np
import sqlite3
import mysql.connector


application = Flask(__name__)
application.secret_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


@application.route("/")
def get_steamer():
    response = requests.get("https://www.fangraphs.com/api/steamer/pitching?key=5sCxU6kRxvCW8VcN", verify=False)
    data = response.json()
    stats = pd.DataFrame(data)
    stats = stats[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "SV", "IP", "ERA", "WHIP", "W", "K", "ER",
         "BB", "H"]]
    stats["Val"] = round((stats["W"] / 10.4) + (stats["K"] / 160) + (stats["SV"] / 26) + (
            ((3.72 - stats["ERA"]) / 3.72) * (stats["IP"] / 150)) + (
                                 ((1.19 - stats["WHIP"]) / 1.19) * (stats["IP"] / 150)), 2)
    stats = stats.sort_values(by='Val', ascending=False)
    stats = stats[stats.IP > 50].round(3)
    stats["POSITION"] = "P"
    stats['Rank'] = stats['Val'].rank(pct=True).round(3)

    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN", verify=False)
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters["Val"] = round((stats_hitters["R"] / 80) + (stats_hitters["HR"] / 23) + (stats_hitters["SB"] / 15) + (
            stats_hitters["RBI"] / 78) + (((stats_hitters["AVG"] - 0.2692) / 0.2692) * (
            stats_hitters["AB"] / 550)), 2)
    stats_hitters = stats_hitters.sort_values(by='Val', ascending=False)
    stats_hitters = stats_hitters[stats_hitters.AB > 200].round(3)
    stats_hitters['Rank'] = stats_hitters['Val'].rank(pct=True).round(3)
    stats = pd.concat([stats, stats_hitters], axis=0, ignore_index=True)
    stats['Rank_All'] = stats['Val'].rank(pct=True).round(3)
    stats['Val'] = (((stats["Rank"] * 4) + stats["Rank_All"]) / 5) * 100
    stats = stats.sort_values(by='Val', ascending=False).round(3)
    return stats.to_json(orient='records')


@application.route("/target/<string:teams>/<string:pitchers>/<string:hitters>")
def get_targets(teams,pitchers,hitters):
    response = requests.get("https://www.fangraphs.com/api/steamer/pitching?key=5sCxU6kRxvCW8VcN", verify=False)
    data = response.json()
    stats = pd.DataFrame(data)
    stats = stats[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "SV", "IP", "ERA", "WHIP", "W", "K", "ER",
         "BB", "H"]]
    stats = stats.sort_values(by='IP', ascending=False)
    stats = stats[stats.IP > 20]
    stats.WHIP = stats.WHIP * -1
    stats.ERA = stats.ERA * -1
    teams = int(teams)
    pitchers = int(pitchers)
    hitters = int(hitters)
    stats_pitchers_head = stats.head((pitchers+2)*teams)
    stats = stats.sort_values(by='SV', ascending=False)
    stats_pitchers_saves = stats.head(3*teams)
    stats = pd.concat([stats_pitchers_head, stats_pitchers_saves], axis=0, ignore_index=True)
    pitching = stats[["IP", "ERA", "WHIP", "W", "K"]].quantile(.7)
    pitching.WHIP = pitching.WHIP * -1
    pitching.ERA = pitching.ERA * -1
    SV = stats[["SV"]].quantile(.825)
    pitching = pd.concat([pitching, SV], axis=0)
    pitching = pd.DataFrame({'Category': pitching.index, 'Target': pitching.values})
    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN", verify=False)
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters = stats_hitters.sort_values(by='AB', ascending=False)
    stats_hitters_top = stats_hitters.head((hitters+3)*teams)
    hitting = stats_hitters_top[["HR", "AB", "AVG", "RBI", "R", "SB"]].quantile(.7)
    hitting = pd.DataFrame({'Category': hitting.index, 'Target': hitting.values})
    targets = pd.concat([pitching, hitting], axis=0, ignore_index=True)
    return targets.to_json(orient='records')

@application.route("/score/<string:model_id>")
def get_score(model_id):
    cnx = mysql.connector.connect(user = 'doadmin',password = 'AVNS_Lkaktbc2QgJkv-oDi60',
    host = 'db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
      port = 25060,
    database = 'crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

        cnx.close()

    else:
        return print("Could not connect")
    results = pd.DataFrame(list(rows), columns=["id", "predictable", "date", "page", "post","prediction","result"])
    results["result"] = pd.to_numeric(results["result"])
    results = results[results[['result']].notnull().all(1)]
    model = results[results.page == model_id]
    brier = brier_score_loss(model["result"], model["prediction"])
    return pd.Series(brier).to_json(orient='records')

@application.route("/template/<string:type_id>")
def get_template(type_id):
    response = requests.get("https://crowdicate.bubbleapps.io/version-test/api/1.1/obj/types")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.bubbleapps.io/version-test/api/1.1/obj/types" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])
    name = results[results._id == type_id]
    name = name["type_text"].values[0]
    if name == "MLB - Strikeouts":
        URL = "https://baseballsavant.mlb.com/probable-pitchers"
        page = requests.get(URL, verify=False)
        soup = BeautifulSoup(page.content, "html.parser")
        links = soup.find_all("a", class_="matchup-link")
        link_list = []
        for link in links:
            test = link["href"]
            splitting = test.split('player_id=')
            link_list.append(splitting[1])
    elif name == "MLB - Game Totals":
        URL = "https://baseballsavant.mlb.com/probable-pitchers"
        page = requests.get(URL, verify=False)
        soup = BeautifulSoup(page.content, "html.parser")
        links = soup.find_all("div", class_="game-info")
        link_list = []
        for link in links:
            link_list.append(link.h2.text.strip())
    else:
        URL = "https://baseballsavant.mlb.com/probable-pitchers"
        page = requests.get(URL, verify=False)
        soup = BeautifulSoup(page.content, "html.parser")
        links = soup.find_all("div", class_="game-info")
        link_list = []
        for link in links:
            test = link.h2.text.strip()
            splitting = test.split(' @ ')
            link_list.append(splitting[0])
            link_list.append(splitting[1])
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            result = cursor.execute("SELECT * FROM predictables")

            rows = cursor.fetchall()

        cnx.close()

    else:
        return print("Could not connect")
    results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
    results['Date'] = str(datetime.date.today())
    results['prediction'] = ""
    template = results[results.type == name]
    if name == "MLB - Strikeouts":
        template = template[template['player_id'].isin(link_list)]
    elif name == "MLB - Game Totals":
        template = template[template['player'].isin(link_list)]
    else:
        template = template[template['player'].isin(link_list)]
    template = template[
        ["id","amount", "player_id", "player", "Date","prediction"]]
    return template.to_json(orient='records')

@application.route("/predictions/<string:post_id>")
def get_predictions(post_id):
    response = requests.get("https://crowdicate.bubbleapps.io/version-test/api/1.1/obj/posts")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.bubbleapps.io/version-test/api/1.1/obj/posts" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])
    post = results[results._id == post_id]
    url = "https:" + post["file_file"].values[0]
    data = pd.read_csv(url)
    data["page"] = post["page_text"].values[0]
    data["post"] = post_id
    data["predictable"] = data["id"]
    data = data[["predictable", "prediction", "page", "date", "post"]]
    data["id"] = [uuid.uuid4().hex for _ in range(len(data.index))]
    data = data[["id", "predictable", "date", "page", "post", "prediction"]]
    con = sqlite3.connect("Crowdicate.db")
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            cursor.executemany("""INSERT INTO predictions
                              (id,predictable,date,page,post,prediction) 
                              VALUES (%s,%s,%s,%s,%s,%s);""",list(data.itertuples(index=False, name=None)))

            cnx.commit()

        cnx.close()
        return "success"

    else:
        return "Could not connect"

@application.route("/aggregate/<string:post_id>/<string:page>/<string:type>")
def get_aggregate(post_id,page,type):
    response = requests.get("https://crowdicate.bubbleapps.io/version-test/api/1.1/obj/types")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.bubbleapps.io/version-test/api/1.1/obj/types" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])
    name = results[results._id == type]
    name = name["type_text"].values[0]
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                              host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                              port=25060,
                              database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

            result_b = cursor.execute("SELECT * FROM predictables")

            types = cursor.fetchall()

            results = pd.DataFrame(list(rows),columns=["id", "predictable", "date", "page", "post","prediction","result"])
            results = results[results['date'] == str(datetime.date.today().strftime("%m/%d/%Y"))]
            group = results.groupby(['predictable', 'date'])['prediction'].agg({'mean'}).reset_index()
            predictables = pd.DataFrame(list(types), columns=["id", "amount", "player", "player_id", "type"])
            group = group.merge(predictables[["id","type"]],how='left', left_on='predictable', right_on='id')
            group = group[group.type == name]
            group["id"] = [uuid.uuid4().hex for _ in range(len(group.index))]
            group["post"] = post_id
            group["page"] = page
            group["prediction"] = group["mean"]
            group = group[["id", "predictable", "date", "page", "post","prediction"]]
            cursor.executemany("""INSERT INTO predictions
                                          (id,predictable,date,page,post,prediction) 
                                          VALUES (%s,%s,%s,%s,%s,%s);""", list(group.itertuples(index=False, name=None)))
            cnx.commit()

        cnx.close()
        return "success"

    else:
        return "Could not connect"





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    application.run()

