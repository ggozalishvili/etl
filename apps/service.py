import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import pandas as pd
import pathlib
import sqlalchemy
from sqlalchemy import create_engine, MetaData, select, Table
from app import app
import dash_table
import dash
from datetime import datetime as dt
import dash_bootstrap_components as dbc
from dash_table.Format import Format, Scheme, Trim
from app import connection, engine
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy(app.server)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
FONT_AWESOME = (
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
)



# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../datasets").resolve()

def service_calculation():
    #engine = sqlalchemy.create_engine('postgresql://oswrsssbmcdtsa:ad6cca6bc8a6d58a80313746f2f7ad22b12ae65d7f75b447f7b785b27845d9e8@ec2-176-34-211-0.eu-west-1.compute.amazonaws.com:5432/d6j6etm7h53s01')
    metadata = MetaData()
    #connection = engine.connect()

    odo_table = Table('odo', metadata, autoload=True,
                      autoload_with=engine)
    stmt_odo = select([odo_table.columns.plate, odo_table.columns.calc_odo])
    results_odo = connection.execute(stmt_odo).fetchall()
    odo = pd.DataFrame(results_odo)
    odo.columns = ['plate', 'odometer']

    # service query
    service_table = Table('service', metadata, autoload=True,
                          autoload_with=engine)
    stmt_service = select([service_table])
    results_service = connection.execute(stmt_service).fetchall()
    service = pd.DataFrame(results_service)
    service.columns = ['plate', 'service_odometer']

    service_joined = odo.merge(service, on='plate', how='left')
    # service_joined['odometer'] = service_joined['odometer'].fillna(0).astype(int)
    # service_joined['service_odometer'] = service_joined['service_odometer'].fillna(0).astype(int)
    service_joined['remaining_km'] = service_joined['service_odometer'] - service_joined['odometer']

    sc_mapping_table = Table('sc_mapping', metadata, autoload=True,
                             autoload_with=engine)
    stmt_sc_mapping = select([sc_mapping_table])
    results_sc_mapping = connection.execute(stmt_sc_mapping).fetchall()
    sc_mapping = pd.DataFrame(results_sc_mapping)
    sc_mapping.columns = ['plate', 'service_center', 'region']

    service_joined = service_joined.merge(sc_mapping, on='plate', how='left')


    return service_joined

a=service_calculation()


layout = dbc.Container([
    dcc.Interval(id='interval_pg5', interval=86400000 * 7, n_intervals=0),  # activated once/week or when page refreshed
    dbc.Row([
        dbc.Col(html.H3("ავტოტრანსპორტის სერვის რეპორტი",
                        className='text-center text-primary mb-4'), width=12)
    ]),
    dbc.Row([dbc.Col([
            dash_table.DataTable(
                id='service_table',
                data=a.to_dict('records'),
                # columns=[{'id': c, 'name': c} for c in aggregated_data_reisebi.columns],
                columns=[{'id': "region", 'name': "region"},
                         {'id': "service_center", 'name': "service_center"},
                         {'id': "plate", 'name': "plate"},
                        dict(id='odometer', name='odometer', type='numeric', format=Format(precision=2, scheme=Scheme.fixed)),
                        dict(id='service_odometer', name='service_odometer', type='numeric', format=Format(precision=2, scheme=Scheme.fixed)),
                        dict(id='remaining_km', name='remaining_km', type='numeric', format=Format(precision=2, scheme=Scheme.fixed)),
                         ],
                editable=False,  # allow editing of data inside all cells
                cell_selectable="False",
                filter_action="native",  # allow filtering of data by user ('native') or not ('none')
                sort_action="native",  # enables data to be sorted per-column by user or not ('none')
                sort_mode="single",  # sort across 'multi' or 'single' columns
                # column_selectable="multi",  # allow users to select 'multi' or 'single' columns
                # row_selectable="multi",  # allow users to select 'multi' or 'single' rows
                # row_deletable=True,  # choose if user can delete a row (True) or not (False)
                selected_columns=[],  # ids of columns that user selects
                selected_rows=[],  # indices of rows that user selects
                page_action="native",  # all data is passed to the table up-front or not ('none')
                fixed_rows={'headers': True},
                page_current=0,  # page number that user is on
                style_table={'height': '600px', 'overflowY': 'auto'},
                page_size=100,  # number of rows visible per page
                style_cell={  # ensure adequate header width when text is shorter than cell's text
                    'minWidth': 95, 'maxWidth': 105, 'width': 105
                },
                style_cell_conditional=[  # align text columns to left. By default they are aligned to right
                    {
                        'if': {'column_id': c},
                        'textAlign': 'center'
                    } for c in ['odometer', 'service_odometer','remaining_km']
                ],
                style_data={  # overflow cells' content into multiple lines
                    'whiteSpace': 'normal',
                    'height': 'auto'
                }
            )
    ], width={'size': 8, 'offset': 0}),
        ],
    no_gutters=False, justify='center', style={'marginBottom': '2em'}),
],fluid=True)

@app.callback(
    Output(component_id='service_table', component_property='data'),
    Input('interval_pg5', 'n_intervals')
)
def service_rows(n_intervals):
    service = service_calculation()
    return service.to_dict('records')
