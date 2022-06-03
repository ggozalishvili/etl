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
app.server.config["SQLALCHEMY_DATABASE_URI"] = "admubvebwkovxe:28d8b50b757940fa5575af3d57a7506220efadb31603aa50788e5cd5ad2dab71@ec2-52-48-159-67.eu-west-1.compute.amazonaws.com:5432/d2dhn8fnd4elon"

# url = "ec2-52-48-159-67.eu-west-1.compute.amazonaws.com:5432/d2dhn8fnd4elon"
# user = "admubvebwkovxe"
# password = '28d8b50b757940fa5575af3d57a7506220efadb31603aa50788e5cd5ad2dab71'

#Test
url = "localhost:5432/geogps"
user = "postgres"
password = '123456'



posgtres = PostgresConnection(url, user, password)

connection, engine = posgtres.create_connection()

# auth = dash_auth.BasicAuth(
#     app,
#     {'bugsbunny': 'topsecret',
#      'pajaroloco': 'unsecreto'}
# )
