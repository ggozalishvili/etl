import dash
import dash_bootstrap_components as dbc
import dash_auth

# meta_tags are required for the app layout to be mobile responsive
from apps.db_connection import PostgresConnection

app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )
server = app.server

app.server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# for your home PostgreSQL test table
app.server.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://postgres:123456@localhost:5432/geogps"

url = "ec2-176-34-211-0.eu-west-1.compute.amazonaws.com:5432/d6j6etm7h53s01"
user = "oswrsssbmcdtsa"
password = 'ad6cca6bc8a6d58a80313746f2f7ad22b12ae65d7f75b447f7b785b27845d9e8'


posgtres = PostgresConnection(url,user, password)

connection, engine = posgtres.create_connection()

# auth = dash_auth.BasicAuth(
#     app,
#     {'bugsbunny': 'topsecret',
#      'pajaroloco': 'unsecreto'}
# )
