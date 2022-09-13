import pathlib
from datetime import datetime as dt

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
from dash.dependencies import Input, Output
from dash_table.Format import Format, Scheme
from sqlalchemy import MetaData, Table
from sqlalchemy import and_
from sqlalchemy.sql import select

from app import app
from app import connection, engine, db

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
FONT_AWESOME = (
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
)

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../datasets").resolve()


menu = pd.read_sql_table('menu', con=db.engine)


def read_data(reg, sc, start, end):
    metadata = MetaData()

    # reisebi query
    reisebi_table = Table('reis', metadata, autoload=True, autoload_with=engine)
    stmt = select([reisebi_table]).where(
        and_(reisebi_table.c.start_time >= start, reisebi_table.c.end_time <= end, reisebi_table.c.region == reg,
             reisebi_table.c.service_center.in_(sc)))
    result = connection.execute(stmt).fetchall()
    reisebi = pd.DataFrame(result)
    reisebi.columns = ['id', 'plate', 'start_date_time', 'start_location', 'end_date_time', 'end_location', 'duration',
                       'milage',
                       'max_speed',
                       'driver', 'fuel_consumed', 'quantity', 'service_center', 'region']
    del reisebi['id']

    ## eco query
    # eco_table = Table('eco', metadata, autoload=True, autoload_with=engine)
    # stmt_eco = select([eco_table]).where(
    #     and_(eco_table.c.start_time >= start, eco_table.c.region == reg,
    #          eco_table.c.service_center.in_(sc)))
    # results_eco = connection.execute(stmt_eco).fetchall()
    # eco = pd.DataFrame(results_eco)
    # eco.columns = ['id', 'plate', 'driver', 'start_date_time', 'location', 'penalty_type', 'penalty_points', 'quantity',
    #                'eco_milage', 'service_center', 'region']
    # del eco['id']

    # skip query
    skip_table = Table('skip', metadata, autoload=True,
                       autoload_with=engine)
    stmt_skip = select([skip_table.columns.plate, skip_table.columns.start_date_time])
    results_skip = connection.execute(stmt_skip).fetchall()
    skip = pd.DataFrame(results_skip)
    skip.columns = ['plate', 'start_date_time']


    skip['start_date_time'] = pd.to_datetime(skip['start_date_time'], format='%Y.%m.%dT%H:%M:%S')

    skipped = skip.drop_duplicates()

    reisebi = (
        reisebi.merge(skipped,
                      on=['plate', 'start_date_time'],
                      how='left',
                      indicator=True)
            .query('_merge == "left_only"')
            .drop(columns='_merge')
    )

    return reisebi, skipped # eco,


def update_read(selected_region, selected_sc, start, end):
    global reisebi_data, skip_trace # eco_data,
    reisebi_data, skip_trace = read_data(selected_region, selected_sc, start, end)
    print("reading done")


reisebi_data, skip_trace = read_data('აჭარა', ['ბათუმი', 'რეგიონი', 'ქობულეთი', 'ხელვაჩაური'], '2021-05-01',
                                               '2021-06-12')

# menu query
metadata = MetaData()
menu_table = Table('menu', metadata, autoload=True,
                   autoload_with=engine)
stmt_menu = select([menu_table])
results_skip = connection.execute(stmt_menu).fetchall()
menu = pd.DataFrame(results_skip)
menu.columns = ['region', 'service_center']



layout = dbc.Container([
    dcc.Interval(id='interval_pg1', interval=86400000 * 7, n_intervals=0),  # activated once/week or when page refreshed
    html.Br(),
    dbc.Row([

        dbc.Col([dcc.DatePickerRange(
            id='my-date-picker-range1',  # ID to be used for callback
            calendar_orientation='horizontal',  # vertical or horizontal
            day_size=39,  # size of calendar image. Default is 39
            end_date_placeholder_text="Return",  # text that appears when no end date chosen
            with_portal=False,  # if True calendar will open in a full screen overlay portal
            first_day_of_week=0,  # Display of calendar when open (0 = Sunday)
            reopen_calendar_on_clear=True,
            is_RTL=False,  # True or False for direction of calendar
            clearable=True,  # whether or not the user can clear the dropdown
            number_of_months_shown=1,  # number of months shown when calendar is open
            min_date_allowed=dt(2022, 1, 1),  # minimum date allowed on the DatePickerRange component
            max_date_allowed=dt(2022, 10, 1),  # maximum date allowed on the DatePickerRange component
            initial_visible_month=dt(2022, 7, 1),  # the month initially presented when the user opens the calendar
            start_date=dt(2022, 6, 1).date(),
            end_date=dt(2022, 10, 1).date(),
            display_format='MMM Do, YY',  # how selected dates are displayed in the DatePickerRange component.
            month_format='MMMM, YYYY',  # how calendar headers are displayed when the calendar is opened.
            minimum_nights=2,  # minimum number of days between start and end date
            persistence=True,
            persisted_props=['start_date'],
            persistence_type='session',  # session, local, or memory. Default is 'local'
            updatemode='singledate'  # singledate or bothdates. Determines when callback is triggered
        ),
            html.Br(),
            dcc.Dropdown(
                id='region_dropdown1',
                options=[{'label': s, 'value': s} for s in sorted(menu['region'].dropna().unique())],
                # options=[{'label': s, 'value': s} for s in sorted(['აჭარა','ქართლი'])],
                # multi=True,
                value='აჭარა',
                clearable=False
            ),
            dcc.Dropdown(id='sc_dropdown1', options=[], multi=True, value='ბათუმი'),

        ]
            , width={'size': 2, 'offset': 0}),

        dbc.Col([
            html.H3("აგრეგაციები მძღოლების მიხედვით"),
            dash_table.DataTable(
                id='aggregate_drivers_table',
                # data=aggregated_data_reisebi.to_dict('records'),
                # columns=[{'id': c, 'name': c} for c in aggregated_data_reisebi.columns],
                columns=[  # {'id': "service_center", 'name': "service_center"},
                    {'id': "driver", 'name': "driver"},
                    {'id': "plate", 'name': "plate"},
                    # {'id': "milage", 'name': "milage"},
                    dict(id='milage', name='milage', type='numeric', format=Format(precision=2, scheme=Scheme.fixed)),
                    {'id': "max_speed", 'name': "max_speed"},
                    {'id': "speed_limit_exceed", 'name': "speed_limit_exceed"},
                    #{'id': "penalty_points", 'name': "penalty_points"},
                    # {'id': "id", 'name': "id"},
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
                style_table={'height': '500px', 'overflowY': 'auto'},
                page_size=70,  # number of rows visible per page
                style_cell={  # ensure adequate header width when text is shorter than cell's text
                    'minWidth': 95, 'maxWidth': 105, 'width': 105
                },
                style_cell_conditional=[  # align text columns to left. By default they are aligned to right
                    {
                        'if': {'column_id': c},
                        'textAlign': 'left'
                    } for c in ['start_location', 'end_location']
                ],
                style_data={  # overflow cells' content into multiple lines
                    'whiteSpace': 'normal',
                    'height': 'auto'
                }
            )], width={'size': 5, 'offset': 0}),

        dbc.Col([
            html.H3("გარბენის დეტალები მანქანაზე"),
            dash_table.DataTable(
                id='cars_table',
                # data=aggregated_data_reisebi.to_dict('records'),
                # columns=[{'id': c, 'name': c} for c in aggregated_data_reisebi.columns],
                columns=[
                    # {'id': "plate", 'name': "plate"},
                    {'id': "start_date_time", 'name': "start_date_time"},
                    {'id': "start_location", 'name': "start_location"},
                    dict(id='milage', name='milage', type='numeric', format=Format(precision=2, scheme=Scheme.fixed)),
                    {'id': "max_speed", 'name': "max_speed"},
                    {'id': "plate", 'name': "plate"},
                    {'id': "driver", 'name': "driver"},
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
                style_table={'height': '500px', 'overflowY': 'auto'},
                page_size=70,  # number of rows visible per page
                style_cell={  # ensure adequate header width when text is shorter than cell's text
                    'minWidth': 95, 'maxWidth': 105, 'width': 105
                },
                style_cell_conditional=[  # align text columns to left. By default they are aligned to right
                    {
                        'if': {'column_id': c},
                        'textAlign': 'left'
                    } for c in ['start_location', 'end_location']
                ],
                style_data={  # overflow cells' content into multiple lines
                    'whiteSpace': 'normal',
                    'height': 'auto'
                }
            )], width={'size': 5, 'offset': 0}),

        html.Div(id='placeholder1', children=[]),
        dcc.Store(id="store1", data=0),
        dcc.Store(id="store99", data=0),
        dcc.Interval(id='interval1', interval=1000)
    ], no_gutters=False, justify='left', style={'marginBottom': '2em'}),
    html.Br(),

    dbc.Row([
        html.Div([
            # html.Button('Read from SQL', id='submit-val', n_clicks=0),
            # html.A(html.Button('Refresh Data'), href='/apps/drivers'),
        ])
    ], no_gutters=False, justify='left', style={'marginBottom': '2em'}),
], fluid=True)


@app.callback(
    Output('sc_dropdown1', 'options'),
    Input('region_dropdown1', 'value'),
)
def get_sc_options(region_dropdown):
    region_un = menu[menu['region'] == region_dropdown]
    return [{'label': i, 'value': i} for i in sorted(region_un.service_center.unique())]


@app.callback(
    Output('sc_dropdown1', 'value'),
    Input('sc_dropdown1', 'options'),
)
def get_sc(sc_drop):
    return [x['value'] for x in sc_drop]


@app.callback(
    Output(component_id='aggregate_drivers_table', component_property='data'),
    Input(component_id='region_dropdown1', component_property='value'),
    Input(component_id='sc_dropdown1', component_property='value'),
    Input('my-date-picker-range1', 'start_date'),
    Input('my-date-picker-range1', 'end_date'),
    Input('interval_pg1', 'n_intervals')
)
def update_aggregate_drv_rows(selected_region, selected_sc, start, end, n_intervals):
    update_read(selected_region, selected_sc, start, end)
    print(selected_region, selected_sc, start, end)
    end
    # data_reis = reisebi_data[(reisebi_data.region == selected_region) & (reisebi_data.service_center.isin(selected_sc)) & ((reisebi_data.start_date_time >= start) & (reisebi_data.start_date_time <= end))]
    # data_eco = eco_data[(eco_data.region == selected_region) & (eco_data.service_center.isin(selected_sc)) & ((eco_data.start_date_time >= start) & (eco_data.start_date_time <= end))]
    # a = data_reis[['service_center', 'driver', 'plate']].reset_index()
    # del a['index']
    # b=a.drop_duplicates()
    agr_to_table = reisebi_data.groupby(['driver', 'plate']).agg(
        milage=('milage', sum),
        max_speed=('max_speed', max),
        speed_limit_exceed=("max_speed", lambda x: x[x >= 90].count())
    )
    agr_to_table = agr_to_table.reset_index()
    # agr_eco = eco_data.groupby('driver').agg(
    #     penalty_points=('penalty_points', sum),
    # )
    # agr_to_table = agr_raisebi.merge(agr_eco, on='driver', how='left')

    agr_to_table['id'] = agr_to_table['plate'] + '/' + agr_to_table['driver']
    agr_to_table.set_index('id', inplace=True, drop=False)
    return agr_to_table.to_dict('records')


@app.callback(
    Output(component_id='cars_table', component_property='data'),
    [Input(component_id='region_dropdown1', component_property='value'),
     Input(component_id='sc_dropdown1', component_property='value'),
     Input('my-date-picker-range1', 'start_date'),
     Input('my-date-picker-range1', 'end_date'),
     Input('interval_pg1', 'n_intervals'),
     Input(component_id='aggregate_drivers_table', component_property='selected_cells'), ]
)
def car_details(selected_region, selected_sc, start, end, n_intervals, slctd_cell):
    input_cell = [] if slctd_cell is None else slctd_cell


    a_key = "row_id"
    values_of_key = [a_dict[a_key] for a_dict in input_cell]
    print(values_of_key)
    pl_id = [i.split('/')[0] for i in values_of_key]
    drv_id = [i.split('/')[1] for i in values_of_key]

    df = reisebi_data[
        (reisebi_data.region == selected_region) & (reisebi_data.service_center.isin(selected_sc)) & (
                (reisebi_data.start_date_time >= start) & (reisebi_data.start_date_time <= end))]

    # filtered_cars_plates = df.query('driver in @drv_id')
    filtered_cars_plates = df.query('plate in @pl_id & driver in @drv_id')

    print(filtered_cars_plates)
    selected_columns_df = filtered_cars_plates[
        ["service_center", "max_speed", "start_date_time", "start_location", "milage", "plate", "driver"]]
    print(selected_columns_df)
    return selected_columns_df.to_dict('records')
