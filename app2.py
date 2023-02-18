import operator
import string
import random
import datetime
import uuid

from flask import Flask, render_template, request, session, redirect
import requests
import bcrypt
import json
import http.client
import pandas as pd

from common.database import Database
from models.adjustment import Adjust
from models.adjustments import Adjustment
from models.target import Target
from models.users import User

application = Flask(__name__)
application.secret_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

@application.before_first_request
def initialize_database():
    Database.initialize()


@application.route("/")
def login_page():
    return render_template("login.html", text="")


@application.route("/register")
def register_page():
    return render_template("register.html", text="")


@application.route("/loginvalid", methods=['POST'])
def login_user():
    email = request.form['email']
    password = request.form['password'].encode("utf-8")
    if User.get_by_email(email) is not None:
        hashed = User.get_by_email(email).password
        if bcrypt.checkpw(password, hashed):
            User.login(email)
            user = User.get_by_email(email)
            session['username'] = user.username
            return redirect('/home')
        else:
            session['email'] = None
            return render_template("login.html", text="Incorrect username or password")
    else:
        return render_template("login.html", text="No User Associated With That Email")

@application.route("/projections")
def home_page():
    username = session['username']
    targets = Target.get_by_user(username)
    response = requests.get("https://www.fangraphs.com/api/steamer/pitching?key=5sCxU6kRxvCW8VcN")
    data = response.json()
    stats = pd.DataFrame(data)
    stats = stats[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "SV", "IP", "ERA", "WHIP", "W", "K", "ER",
         "BB", "H"]]
    stats["Val"] = round((stats["W"] / 9) + (stats["K"] / 150) + (stats["SV"] / 18) + (
            ((3.85 - stats["ERA"]) / 3.85) * (stats["IP"] / 150)) + (
                                 ((1.2 - stats["WHIP"]) / 1.2) * (stats["IP"] / 150)), 2)
    stats = stats.sort_values(by='Val', ascending=False)
    stats = stats[stats.IP > 50].round(3)
    stats["POSITION"] = "P"
    stats['Rank'] = stats['Val'].rank(pct=True).round(3)

    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN")
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters["Val"] = round((stats_hitters["R"] / 75) + (stats_hitters["HR"] / 22) + (stats_hitters["SB"] / 15) + (
            stats_hitters["RBI"] / 75) + (((stats_hitters["AVG"] - 0.2692) / 0.2692) * (
            stats_hitters["AB"] / 500)), 2)
    stats_hitters = stats_hitters.sort_values(by='Val', ascending=False)
    stats_hitters = stats_hitters[stats_hitters.AB > 200].round(3)
    stats_hitters['Rank'] = stats_hitters['Val'].rank(pct=True).round(3)
    stats = pd.concat([stats, stats_hitters], axis=0, ignore_index=True)
    stats['Rank_All'] = stats['Val'].rank(pct=True).round(3)
    stats['Val'] = (((stats["Rank"] * 2) + stats["Rank_All"]) / 3) * 100
    stats = stats.sort_values(by='Val', ascending=False).round(3)
    stats['Even'] = 'checked'
    return render_template("home.html", stats=stats,default="Adjustment Name",targets=targets)

@application.route("/projection_adjust/<string:adjustment_id>")
def projections_adjust(adjustment_id):
    username = session['username']
    targets = Target.get_by_user(username)
    response = requests.get("https://www.fangraphs.com/api/steamer/pitching?key=5sCxU6kRxvCW8VcN")
    data = response.json()
    stats = pd.DataFrame(data)
    stats = stats[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "SV", "IP", "ERA", "WHIP", "W", "K", "ER",
         "BB", "H"]]
    stats = stats.rename(columns={"H": "H-P"})
    stats["Val"] = round((stats["W"] / 9) + (stats["K"] / 150) + (stats["SV"] / 18) + (
            ((3.85 - stats["ERA"]) / 3.85) * (stats["IP"] / 150)) + (
                                 ((1.2 - stats["WHIP"]) / 1.2) * (stats["IP"] / 150)), 2)
    stats = stats.sort_values(by='Val', ascending=False)
    stats = stats[stats.IP > 50].round(3)
    stats["POSITION"] = "P"
    stats['Rank'] = stats['Val'].rank(pct=True).round(3)

    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN")
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters["Val"] = round((stats_hitters["R"] / 75) + (stats_hitters["HR"] / 22) + (stats_hitters["SB"] / 15) + (
            stats_hitters["RBI"] / 75) + (((stats_hitters["AVG"] - 0.2692) / 0.2692) * (
            stats_hitters["AB"] / 500)), 2)
    stats_hitters = stats_hitters.sort_values(by='Val', ascending=False)
    stats_hitters = stats_hitters[stats_hitters.AB > 200].round(3)
    stats_hitters['Rank'] = stats_hitters['Val'].rank(pct=True).round(3)
    stats = pd.concat([stats, stats_hitters], axis=0, ignore_index=True)
    stats['Rank_All'] = stats['Val'].rank(pct=True).round(3)
    stats['Val'] = ((stats["Rank"] + stats["Rank_All"]) / 2) * 100
    stats = stats.sort_values(by='Val', ascending=False).round(3)

    adjustments = Adjustment.get_by_adjustment_id(adjustment_id)
    adjust = Adjust.get_by_id(adjustment_id)
    default = adjust.adjustment_name
    adjustments = pd.DataFrame([vars(f) for f in adjustments])
    stats = pd.merge(stats, adjustments[['mlbamid', 'adjustment']],how='left')
    stats['adjustment'] = stats['adjustment'].fillna(0)
    stats["adjust"] = 0.05
    stats["adjustment"] = pd.to_numeric(stats["adjustment"])
    stats["adjust"] = stats["adjust"] * stats["adjustment"]
    stats['HR_AB'] = (stats["HR"] / stats["AB"])
    stats['SB_AB'] = (stats["SB"] / stats["AB"])
    stats['R_AB'] = (stats["R"] / stats["AB"])
    stats['RBI_AB'] = (stats["RBI"] / stats["AB"])
    stats['H_AB'] = (stats["H"] / stats["AB"])
    stats["AB"] - stats["AB"] + (stats["AB"] * (stats["adjust"] / 2))
    stats['HR'] = (stats["HR_AB"] + (stats["HR_AB"] * stats["adjust"])) * stats["AB"]
    stats['SB'] = (stats["SB_AB"] + (stats["SB_AB"] * stats["adjust"])) * stats["AB"]
    stats['R'] = (stats["R_AB"] + (stats["R_AB"] * stats["adjust"])) * stats["AB"]
    stats['RBI'] = (stats["RBI_AB"] + (stats["RBI_AB"] * stats["adjust"])) * stats["AB"]
    stats['H'] = (stats["H_AB"] + (stats["H_AB"] * stats["adjust"])) * stats["AB"]
    stats['AVG'] = stats['H'] / stats['AB']

    stats['W_IP'] = (stats["W"] / stats["IP"])
    stats['K_IP'] = (stats["K"] / stats["IP"])
    stats['SV_IP'] = (stats["SV"] / stats["IP"])
    stats['R_IP'] = (stats["ER"] / stats["IP"])
    stats['H_IP'] = (stats["H-P"] / stats["IP"])
    stats['BB_IP'] = (stats["BB"] / stats["IP"])
    stats["IP"] = stats["IP"] + (stats["IP"] * (stats["adjust"] / 2))
    stats['W'] = (stats["W_IP"] + (stats["W_IP"] * stats["adjust"])) * stats["IP"]
    stats['K'] = (stats["K_IP"] + (stats["K_IP"] * stats["adjust"])) * stats["IP"]
    stats['SV'] = (stats["SV_IP"] + (stats["SV_IP"] * stats["adjust"])) * stats["IP"]
    stats['ER'] = (stats["R_IP"] - (stats["R_IP"] * stats["adjust"])) * stats["IP"]
    stats['BB'] = (stats["BB_IP"] - (stats["BB_IP"] * stats["adjust"])) * stats["IP"]
    stats['H-P'] = (stats["H_IP"] - (stats["H_IP"] * stats["adjust"])) * stats["IP"]
    stats['WHIP'] = (stats['H-P'] + stats['BB']) / stats['IP']
    stats['ERA'] = (stats['ER'] * 9) / stats['IP']
    stats = stats.round(3)
    stats['P_Group'] = stats['POSITION'].apply(lambda x: 'Pitcher' if x == 'P' else 'Hitter')
    stats['Down2'] = stats['adjustment'].apply(lambda x: 'checked' if x == -1 else '')
    stats['Down1'] = stats['adjustment'].apply(lambda x: 'checked' if x == -0.5 else '')
    stats['Even'] = stats['adjustment'].apply(lambda x: 'checked' if x == 0 else '')
    stats['Up1'] = stats['adjustment'].apply(lambda x: 'checked' if x == 0.5 else '')
    stats['Up2'] = stats['adjustment'].apply(lambda x: 'checked' if x == 1 else '')
    stats['up'] = stats['adjustment'].apply(lambda x: 'up' if x > 0 else '')
    stats['down'] = stats['adjustment'].apply(lambda x: 'down' if x < 0 else '')

    stats = stats.fillna(0)
    stats["Val"] = round((stats["R"] / 75) + (stats["HR"] / 22) + (stats["SB"] / 15) + (
            stats["RBI"] / 75) + (((stats["AVG"] - 0.2692) / 0.2692) * (
            stats["AB"] / 500)), 2) + round((stats["W"] / 9) + (stats["K"] / 150) + (stats["SV"] / 18) + (
            ((3.85 - stats["ERA"]) / 3.85) * (stats["IP"] / 150)) + (
                                                    ((1.2 - stats["WHIP"]) / 1.2) * (stats["IP"] / 150)), 2)
    stats['Val'] = ((stats.groupby('P_Group')['Val'].rank(pct=True) + stats['Val'].rank(pct=True).round(3)) / 2) * 100
    stats = stats.sort_values(by='Val', ascending=False).round(3)
    return render_template("home.html", stats=stats,default=default,targets=targets)

@application.route("/adjust", methods=['POST'])
def adjust_players():
    response = requests.get("https://www.fangraphs.com/api/steamer/pitching?key=5sCxU6kRxvCW8VcN")
    data = response.json()
    stats = pd.DataFrame(data)
    stats = stats[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "SV", "IP", "ERA", "WHIP", "W", "K", "ER",
         "BB", "H"]]
    stats = stats.rename(columns={"H": "H-P"})
    stats["Val"] = round((stats["W"] / 9) + (stats["K"] / 150) + (stats["SV"] / 18) + (
            ((3.85 - stats["ERA"]) / 3.85) * (stats["IP"] / 150)) + (
                                 ((1.2 - stats["WHIP"]) / 1.2) * (stats["IP"] / 150)), 2)
    stats = stats.sort_values(by='Val', ascending=False)
    stats = stats[stats.IP > 50].round(3)
    stats["POSITION"] = "P"
    stats['Rank'] = stats['Val'].rank(pct=True).round(3)

    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN")
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters["Val"] = round((stats_hitters["R"] / 75) + (stats_hitters["HR"] / 22) + (stats_hitters["SB"] / 15) + (
            stats_hitters["RBI"] / 75) + (((stats_hitters["AVG"] - 0.2692) / 0.2692) * (
            stats_hitters["AB"] / 500)), 2)
    stats_hitters = stats_hitters.sort_values(by='Val', ascending=False)
    stats_hitters = stats_hitters[stats_hitters.AB > 200].round(3)
    stats_hitters['Rank'] = stats_hitters['Val'].rank(pct=True).round(3)
    stats = pd.concat([stats, stats_hitters], axis=0, ignore_index=True)
    stats['Rank_All'] = stats['Val'].rank(pct=True).round(3)
    stats['Val'] = ((stats["Rank"] + stats["Rank_All"]) / 2) * 100
    stats = stats.sort_values(by='Val', ascending=False).round(3)

    list = stats["mlbamid"].tolist()
    list = set(list)
    list = sorted(list)
    adjusts = []
    for i in list:
        test = request.form[str(i)]
        adjusts.append(test)
    adjustments = zip(list, adjusts)
    adjustments = pd.DataFrame(adjustments, columns=['mlbamid', 'adjustment'])
    adjustments["username"] = session["username"]
    adjustments["adjustment_name"] = request.form['adjust_name']
    saved = adjustments[adjustments.adjustment != "0.0"]
    if Adjust.get_by_user_name(session["username"],request.form['adjust_name']) is None:
        id = uuid.uuid4().hex
        Database.insert("adjustment", {
            "username": session["username"],
            "adjustment_name": request.form['adjust_name'],
            "_id": id
        })
        adjust = Adjust.get_by_id(id)
    else:
        adjust = Adjust.get_by_user_name(session["username"],request.form['adjust_name'])

    for index, row in saved.iterrows():
        Database.update_one("adjustments", {"$and":[{"username": row['username']},
                                                    {"adjustment_name": row['adjustment_name']},
                                                    {"adjustment_id": adjust._id},
                                                    {"mlbamid": row['mlbamid']}]},
                       {"$set": {"adjustment": row['adjustment']}})

    stats = pd.merge(stats, adjustments[['mlbamid', 'adjustment']])
    stats["adjust"] = (1 - stats["reliability"])
    stats["adjustment"] = pd.to_numeric(stats["adjustment"])
    stats["adjust"] = stats["adjust"] * stats["adjustment"]
    stats['HR_AB'] = (stats["HR"] / stats["AB"])
    stats['SB_AB'] = (stats["SB"] / stats["AB"])
    stats['R_AB'] = (stats["R"] / stats["AB"])
    stats['RBI_AB'] = (stats["RBI"] / stats["AB"])
    stats['H_AB'] = (stats["H"] / stats["AB"])
    stats["AB"] - stats["AB"] + (stats["AB"] * (stats["adjust"] / 2))
    stats['HR'] = (stats["HR_AB"] + (stats["HR_AB"] * stats["adjust"])) * stats["AB"]
    stats['SB'] = (stats["SB_AB"] + (stats["SB_AB"] * stats["adjust"])) * stats["AB"]
    stats['R'] = (stats["R_AB"] + (stats["R_AB"] * stats["adjust"])) * stats["AB"]
    stats['RBI'] = (stats["RBI_AB"] + (stats["RBI_AB"] * stats["adjust"])) * stats["AB"]
    stats['H'] = (stats["H_AB"] + (stats["H_AB"] * stats["adjust"])) * stats["AB"]
    stats['AVG'] = stats['H'] / stats['AB']

    stats['W_IP'] = (stats["W"] / stats["IP"])
    stats['K_IP'] = (stats["K"] / stats["IP"])
    stats['SV_IP'] = (stats["SV"] / stats["IP"])
    stats['R_IP'] = (stats["ER"] / stats["IP"])
    stats['H_IP'] = (stats["H-P"] / stats["IP"])
    stats['BB_IP'] = (stats["BB"] / stats["IP"])
    stats["IP"] = stats["IP"] + (stats["IP"] * (stats["adjust"] / 2))
    stats['W'] = (stats["W_IP"] + (stats["W_IP"] * stats["adjust"])) * stats["IP"]
    stats['K'] = (stats["K_IP"] + (stats["K_IP"] * stats["adjust"])) * stats["IP"]
    stats['SV'] = (stats["SV_IP"] + (stats["SV_IP"] * stats["adjust"])) * stats["IP"]
    stats['ER'] = (stats["R_IP"] - (stats["R_IP"] * stats["adjust"])) * stats["IP"]
    stats['BB'] = (stats["BB_IP"] - (stats["BB_IP"] * stats["adjust"])) * stats["IP"]
    stats['H-P'] = (stats["H_IP"] - (stats["H_IP"] * stats["adjust"])) * stats["IP"]
    stats['WHIP'] = (stats['H-P'] + stats['BB']) / stats['IP']
    stats['ERA'] = (stats['ER'] * 9) / stats['IP']
    stats = stats.round(3)
    stats['P_Group'] = stats['POSITION'].apply(lambda x: 'Pitcher' if x == 'P' else 'Hitter')
    stats = stats.fillna(0)
    stats["Val"] = round((stats["R"] / 75) + (stats["HR"] / 22) + (stats["SB"] / 15) + (
            stats["RBI"] / 75) + (((stats["AVG"] - 0.2692) / 0.2692) * (
            stats["AB"] / 500)), 2) + round((stats["W"] / 9) + (stats["K"] / 150) + (stats["SV"] / 18) + (
            ((3.85 - stats["ERA"]) / 3.85) * (stats["IP"] / 150)) + (
                                 ((1.2 - stats["WHIP"]) / 1.2) * (stats["IP"] / 150)), 2)
    stats['Val'] = ((stats.groupby('P_Group')['Val'].rank(pct=True) + stats['Val'].rank(pct=True).round(3))/2)*100
    stats = stats.sort_values(by='Val', ascending=False).round(3)
    return render_template("home.html", stats=stats)

@application.route("/home")
def projections_home():
    username = session['username']
    adjustments = Adjust.get_by_user(username)
    targets = Target.get_by_user(username)
    return render_template("projections_home.html", adjustments=adjustments,targets=targets)

@application.route("/adjustment_view/<string:adjustment_id>")
def view_adjustments(adjustment_id):
    adjustments = Adjustment.get_by_adjustment_id(adjustment_id)
    adjustment = Adjust.get_by_id(adjustment_id)
    adjust_id = adjustment._id
    adjust_name = adjustment.adjustment_name
    return render_template("adjustment_view.html", adjustments=adjustments,adjust_id=adjust_id,adjust_name=adjust_name)

@application.route("/target_view/<string:target_id>")
def view_targwts(target_id):
    target = Target.get_by_id(target_id)
    return render_template("target_view.html", target=target)


if __name__ == "__main__":
    application.run()
