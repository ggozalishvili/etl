import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import pathlib
from app import app
import dash_table
import dash
from datetime import datetime as dt
import dash_bootstrap_components as dbc
from flask_sqlalchemy import SQLAlchemy
from flask import Flask
from app import connection, engine

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
FONT_AWESOME = (
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
)

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../datasets").resolve()

db = SQLAlchemy(app.server)


class Product(db.Model):
    __tablename__ = 'service'


    plate = db.Column(db.String(40), nullable=False,primary_key=True)
    odometer = db.Column(db.Integer, nullable=False)


    def __init__(self, plate, odometer):
        self.plate = plate
        self.odometer = odometer




layout = dbc.Container([
    dcc.Interval(id='interval_pg2', interval=86400000*7, n_intervals=0),  # activated once/week or when page refreshed
    dbc.Row([
        dbc.Col(html.H3("დასასერვისებელი ოდომეტრის ჩვენებები",
                        className='text-center text-primary mb-4'), width=12)
    ]),

    # dcc.Interval(id='interval_pg2', interval=86400000*7, n_intervals=0),  # activated once/week or when page refreshed

    dbc.Row([dbc.Col(
        html.Div(id='postgres_datatable2'),)

    ]),
    dbc.Row([
    html.Button('მწკრივის დამატება', id='editing-rows-button', n_clicks=0),
    html.Button('დამახსოვრება', id='save_to_postgres', n_clicks=0),
    ]),
    # Create notification when saving to excel
    html.Div(id='placeholder2', children=[]),
    dcc.Store(id="store2", data=0),
    dcc.Interval(id='interval2', interval=1000),
    ])

@app.callback(Output('postgres_datatable2', 'children'),
              [Input('interval_pg2', 'n_intervals')])
def populate_datatable(n_intervals):
    df = pd.read_sql_table('service', con=connection)
    return [
        dash_table.DataTable(
            id='our-table',
            columns=[{
                         'name': str(x),
                         'id': str(x),
                         'deletable': False,
                         'type': 'numeric',
                     } if x == 'odometer'
                     else {
                'name': str(x),
                'id': str(x),
                'deletable': True,
            }
                     for x in df.columns],
            data=df.to_dict('records'),
            editable=True,
            row_deletable=True,
            filter_action="native",
            sort_action="native",  # give user capability to sort columns
            sort_mode="single",  # sort across 'multi' or 'single' columns
            page_action='none',  # render all of the data at once. No paging.
            style_table={'height': '300px', 'overflowY': 'auto'},
            page_size=70,
            style_cell={'textAlign': 'left', 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px'},
            style_cell_conditional=[
                {
                    'if': {'column_id': c},
                    'textAlign': 'right'
                } for c in ['odometer']
            ]

        ),
    ]

@app.callback(
    Output('our-table', 'data'),
    [Input('editing-rows-button', 'n_clicks')],
    [State('our-table', 'data'),
     State('our-table', 'columns')],
    prevent_initial_call=True)
def add_row(n_clicks, rows, columns):
    if n_clicks > 0:
        rows.append({c['id']: '' for c in columns})
    return rows

@app.callback(
    [Output('placeholder2', 'children'),
     Output("store2", "data")],
    [Input('save_to_postgres', 'n_clicks'),
     Input("interval2", "n_intervals")],
    [State('our-table', 'data'),
     State('store2', 'data')],
    prevent_initial_call=True)
def df_to_csv(n_clicks, n_intervals, dataset, s):
    output = html.Plaintext("The data has been saved to your PostgreSQL database.",
                            style={'color': 'green', 'font-weight': 'bold', 'font-size': 'large'})
    no_output = html.Plaintext("", style={'margin': "0px"})

    input_triggered = dash.callback_context.triggered[0]["prop_id"].split(".")[0]

    if input_triggered == "save_to_postgres":
        s = 6
        pg = pd.DataFrame(dataset)
        pg.to_sql("service", con=connection, if_exists='replace', index=False)
        return output, s
    elif input_triggered == 'interval2' and s > 0:
        s = s - 1
        if s > 0:
            return output, s
        else:
            return no_output, s
    elif s == 0:
        return no_output, s

