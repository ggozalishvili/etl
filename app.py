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
app.server.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://mpxwygfqyhlykh:5ba21ce8429a2bfd66cb38edcaa4d28a276f28843f666edb54aa4f4e9e55f67a@ec2-54-194-211-183.eu-west-1.compute.amazonaws.com:5432/d8l6qs2difee9t"


db = SQLAlchemy(app.server)
engine = db.engine
connection = engine.connect()

app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# auth = dash_auth.BasicAuth(
#     app,
#     {'bugsbunny': 'topsecret',
#      'pajaroloco': 'unsecreto'}
# )
