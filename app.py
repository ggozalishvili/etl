import dash
import dash_bootstrap_components as dbc
import dash_auth

# meta_tags are required for the app layout to be mobile responsive
from flask_sqlalchemy import SQLAlchemy

from apps.db_connection import PostgresConnection

app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )
server = app.server
#app.server.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://postgres:123456@localhost/geogps"

# for your home PostgreSQL test table
app.server.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://cnwcerliarfbvk:0091d0f3d6abbc8f5545f122703dfb43ee0da1b18e2d59282a96cc62373060af@ec2-63-32-248-14.eu-west-1.compute.amazonaws.com:5432/d886nbel7m1jvm"


db = SQLAlchemy(app.server)
engine = db.engine
connection = engine.connect()

app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

