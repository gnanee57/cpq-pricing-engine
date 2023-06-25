import os

from flask import Flask
from flask_apscheduler import APScheduler
from flask_cors import CORS

from models.models import db

app = Flask(__name__)
CORS(app)
scheduler = APScheduler()

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://edfhobs:edfhobs@172.20.0.10:3306/ideation_edf'
app.config['SQLALCHEMY_POOL_SIZE'] = 30
app.config['SQLALCHEMY_POOL_TIMEOUT'] = 3600
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['CUST_SOQ_CAL'] = True
app.config['CPQ_URL'] = 'http://localhost:8080'
app.config['CPQ_AUTH_TOKEN'] = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJLZXRhbiBCaGV0YXJpeWEiLCJwZXJzb25JZCI6IjIiLCJwZXJzb25OYW1lIjoiS2V0YW4gQmhldGFyaXlhIiwiZW1haWwiOiJrZXRhbi5iaGV0YXJpeWFAdGNzLmNvbSIsImNvbnRhY3ROdW1iZXIiOiIxIiwiam9iVGl0bGUiOiJBZG1pbiIsInN0YXJ0RGF0ZSI6IldlZCBEZWMgMzAgMTY6MjY6MzAgSVNUIDIwMjAiLCJlbmREYXRlIjoiTW9uIERlYyAzMCAxNjoyNjoxNSBJU1QgMjAyNCIsImlzc3VlZERhdGUiOiJUaHUgTWFyIDMwIDIxOjI0OjU4IElTVCAyMDIzIn0.CA11FVdjkTq_BPY7j7iGYyb8fOxjFW1s2goMYMsWno3gbF550V9d9Yr587lgt3VrgAXlJjihadNJ2v-lMZIWqQ"
app.config['PREPROCESS_DIR'] = os.path.abspath('//home//edf//projects//EDFCPQ//PRICE_INPUT')
app.config['PROCESSED_OUT'] = os.path.abspath('//app//hobs_python//processed//out')
app.config['LOAD_FACTOR_PATH'] = os.path.abspath('//app//hobs_python//processed//out')
app.config['FORECAST_INPUT'] = os.path.abspath('//app//hobs_python//forecast//input')
app.config['FORECAST_OUT'] = os.path.abspath('//app//hobs_python//forecast//out')
app.config['PRICING_INPUT'] = os.path.abspath('//app//hobs_python//pricing//input')
app.config['PRICING_OUT'] = os.path.abspath('//app//hobs_python//pricing//out')

db.init_app(app)
scheduler.init_app(app)
