import pathlib
from datetime import datetime as dt
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
from dash.dependencies import Input, Output
from dash_table.Format import Format, Scheme
from sqlalchemy import MetaData, Table, case, func
from sqlalchemy import and_
from sqlalchemy.sql import select
from app import app
from app import connection, engine, db
from dash.dependencies import Input, Output
from dash import Dash

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
FONT_AWESOME = (
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
)

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../datasets").resolve()


menu = pd.read_sql_table('menu', con=db.engine)


def read_data(start, end):
    metadata = MetaData()

    # reisebi query
    reisebi_table = Table('reis', metadata, autoload=True, autoload_with=engine)

    stmt_reis = select([reisebi_table.c.region,reisebi_table.c.service_center,reisebi_table.c.driver,
                     (func.sum((reisebi_table.c.milage))).label('milage'),
                     (func.max((reisebi_table.c.max_speed))).label('max_speed')])

    stmt_reis = stmt_reis.where(
             and_(reisebi_table.c.start_time >= start, reisebi_table.c.end_time <= end, reisebi_table.c.driver !='-----'))
    stmt_reis = stmt_reis.group_by(reisebi_table.columns.region, reisebi_table.columns.service_center, reisebi_table.columns.driver)
    result_reis = connection.execute(stmt_reis).fetchall()
    agr_reisebi = pd.DataFrame(result_reis)
    agr_reisebi.columns = ['region','service_center','driver','milage','max_speed']

    # reisebi query2
    reisebi_table2 = Table('reis', metadata, autoload=True, autoload_with=engine)
    stmt_reis2 = select([reisebi_table2.c.region,reisebi_table2.c.service_center,reisebi_table2.c.driver,
                     (func.count((reisebi_table2.c.max_speed))).label('max_speed_cnt')])
    stmt_reis2 = stmt_reis2.where(
             and_(reisebi_table2.c.start_time >= start, reisebi_table2.c.end_time <= end, reisebi_table2.c.driver !='-----'),reisebi_table2.c.max_speed>90)
    stmt_reis2 = stmt_reis2.group_by(reisebi_table2.columns.region, reisebi_table2.columns.service_center,
                                   reisebi_table2.columns.driver)
    result_reis2 = connection.execute(stmt_reis2).fetchall()
    agr_reisebi2 = pd.DataFrame(result_reis2)
    agr_reisebi2.columns = ['region','service_center','driver','speed_exc_cnt']


    # stmt = select([reisebi_table.c.region,reisebi_table.c.service_center,reisebi_table.c.driver,reisebi_table.c.plate,reisebi_table.c.start_time,reisebi_table.c.milage,reisebi_table.c.max_speed]).where(
    #         and_(reisebi_table.c.start_time >= start, reisebi_table.c.end_time <= end, reisebi_table.c.driver !='-----'))
    # result = connection.execute(stmt).fetchall()
    # reisebi = pd.DataFrame(result)
    # reisebi.columns = ['region','service_center','driver','plate','start_date_time',
    #                    'milage',
    #                    'max_speed']

    agr_reisebi=agr_reisebi.merge(agr_reisebi2, on=['region','service_center','driver'], how='left')
    # reisebi = (
    #      agr_reisebi.merge(agr_reisebi2,
    #                    on=['service_center','driver'],
    #                    how='left',
    #                    indicator=True)
    #          .query('_merge == "left_only"')
    #          .drop(columns='_merge').fillna(0))




    # skip query
    skip_table = Table('skip', metadata, autoload=True,
                       autoload_with=engine)
    stmt_skip = select([skip_table.columns.plate, skip_table.columns.start_date_time])
    results_skip = connection.execute(stmt_skip).fetchall()
    skip = pd.DataFrame(results_skip)
    skip.columns = ['plate', 'start_date_time']


    skip['start_date_time'] = pd.to_datetime(skip['start_date_time'], format='%Y.%m.%dT%H:%M:%S')

    skipped = skip.drop_duplicates()



    # reisebi = (
    #     reisebi.merge(skipped,
    #                   on=['plate', 'start_date_time'],
    #                   how='left',
    #                   indicator=True)
    #         .query('_merge == "left_only"')
    #         .drop(columns='_merge')
    # )
    # agr_raisebi = reisebi.groupby(['region', 'service_center', 'driver']).agg(
    #     milage=('milage', sum),
    #     max_speed=('max_speed', max),
    #     speed_limit_exceed=("max_speed", lambda x: x[x >= 90].count())
    #
    # )
    # agr_raisebi = agr_raisebi.reset_index()
    # # data_e=eco_data[(eco_data['driver'] !='-----')]
    # # data_ec = data_e[(data_e['penalty_type'] !='-----')]
    # del [[reisebi]]
    # gc.collect()


    # eco query
    eco_table = Table('eco', metadata, autoload=True, autoload_with=engine)
    stmt_eco = select([eco_table.columns.region,eco_table.columns.service_center,eco_table.columns.driver,
                    (func.sum(
                        case([
                            (eco_table.columns.penalty_type == 'Harsh acceleration', eco_table.columns.penalty_points)
                        ], else_=0))).label('Harsh_acceleration_points'),
                   (func.sum(
                       case([
                           (eco_table.columns.penalty_type == 'Harsh braking', eco_table.columns.penalty_points)
                       ], else_=0))).label('Harsh_braking_points'),
                    (func.sum(
                        case([
                            (eco_table.columns.penalty_type == 'Harsh cornering', eco_table.columns.penalty_points)
                        ], else_=0))).label('Harsh_cornering_points'),
                    (func.sum(
                        case([
                            (eco_table.columns.penalty_type == 'Very harsh acceleration', eco_table.columns.penalty_points)
                        ], else_=0))).label('Very_harsh_acceleration_points'),
                    (func.sum(
                        case([
                            (eco_table.columns.penalty_type == 'Very harsh braking',
                             eco_table.columns.penalty_points)
                        ], else_=0))).label('Very_harsh_braking_points'),
                    (func.sum(
                        case([
                            (eco_table.columns.penalty_type == 'Very harsh cornering',
                             eco_table.columns.penalty_points)
                        ], else_=0))).label('Very_harsh_cornering_points')
                   ])

    # Group By state
    stmt_eco = stmt_eco.where(
        and_(eco_table.c.start_time >= start, eco_table.c.driver != '-----', eco_table.c.penalty_type != '-----'))
    stmt_eco = stmt_eco.group_by(eco_table.columns.region,eco_table.columns.service_center,eco_table.columns.driver)

    # Execute the query and store the results: results
    results_eco = connection.execute(stmt_eco).fetchall()
    agr_eco = pd.DataFrame(results_eco)
    agr_eco.columns = ['region', 'service_center', 'driver','harsh_a_points', 'harsh_b_points', 'harsh_c_points', 'v_harsh_a_points',
                            'v_harsh_b_points', 'v_harsh_c_points']



    # stmt_eco = select([eco_table.c.region, eco_table.c.service_center, eco_table.c.driver, eco_table.c.penalty_type,
    #                    eco_table.c.penalty_points]).where(
    #     and_(eco_table.c.start_time >= start, eco_table.c.driver != '-----', eco_table.c.penalty_type != '-----'))
    # # stmt_eco = select([eco_table.c.plate, db.func.sum(eco_table.c.penalty_points)]).where(and_(eco_table.c.start_time >= start,)).group_by(eco_table.c.plate)
    # results_eco = connection.execute(stmt_eco).fetchall()
    # eco = pd.DataFrame(results_eco)
    # eco.columns = ['region', 'service_center', 'driver', 'penalty_type', 'penalty_points']
    #
    #
    # agr_eco = eco.pivot_table(index=['region', 'service_center', 'driver'], columns='penalty_type',
    #                                values=['penalty_points'], fill_value=0, aggfunc=np.sum)
    # agr_to_table = agr_raisebi.merge(agr_eco, on='driver', how='left')
    # agr_to_table = agr_to_table.fillna(0)
    # agr_to_table = agr_to_table.astype(int, copy=True, errors='ignore')
    # agr_to_table.columns = ['region', 'service_center', 'driver', 'milage', 'max_speed', 'speed_limit_exceed',
    #                         'harsh_a_points', 'harsh_b_points', 'harsh_c_points', 'v_harsh_a_points',
    #                         'v_harsh_b_points', 'v_harsh_c_points']
    agr_to_table = agr_reisebi.merge(agr_eco, on=['region','service_center','driver'], how='left').fillna(0)

    agr_to_table=agr_to_table.astype(int, copy=True, errors='ignore')
    agr_to_table['total_penalty_points'] = agr_to_table['harsh_a_points'] + agr_to_table['harsh_b_points'] + \
                                           agr_to_table['harsh_c_points'] + agr_to_table['v_harsh_a_points'] + \
                                           agr_to_table['v_harsh_b_points'] + agr_to_table['v_harsh_c_points']



    # print(agr_to_table.head())
    # del [[eco]]
    # gc.collect()
    return agr_to_table



# menu query
metadata = MetaData()
menu_table = Table('menu', metadata, autoload=True,
                   autoload_with=engine)
stmt_menu = select([menu_table])
results_skip = connection.execute(stmt_menu).fetchall()
menu = pd.DataFrame(results_skip)
menu.columns = ['region', 'service_center']

#df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 1, 5, 6], "c": ["x", "x", "y", "y"]})

layout = dbc.Container([
    dcc.Interval(id='interval_pg2', interval=86400000 * 7, n_intervals=0),  # activated once/week or when page refreshed
    html.Br(),
    dbc.Row([

        dbc.Col([dcc.DatePickerRange(
            id='my-date-picker-range2',  # ID to be used for callback
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
            max_date_allowed=dt(2022, 8, 1),  # maximum date allowed on the DatePickerRange component
            initial_visible_month=dt(2022, 1, 1),  # the month initially presented when the user opens the calendar
            start_date=dt(2022, 6, 1).date(),
            end_date=dt(2022, 8, 1).date(),
            display_format='MMM Do, YY',  # how selected dates are displayed in the DatePickerRange component.
            month_format='MMMM, YYYY',  # how calendar headers are displayed when the calendar is opened.
            minimum_nights=2,  # minimum number of days between start and end date
            persistence=True,
            persisted_props=['start_date'],
            persistence_type='session',  # session, local, or memory. Default is 'local'
            updatemode='singledate'  # singledate or bothdates. Determines when callback is triggered
        ),


        ]
            , width={'size': 2, 'offset': 0}),
        ], no_gutters=False, justify='left', style={'marginBottom': '2em'}),
    dbc.Row([
        dbc.Col([
            html.Br(),
            ], width={'size': 12, 'offset': 0}),
            ], no_gutters=False, justify='left', style={'marginBottom': '2em'}),
    dbc.Row([
        dbc.Col([
            html.Br(),
        ], width={'size': 12, 'offset': 0}),
    ], no_gutters=False, justify='left', style={'marginBottom': '2em'}),
    dbc.Row([
        dbc.Col([
            html.Br(),
        ], width={'size': 12, 'offset': 0}),
    ], no_gutters=False, justify='left', style={'marginBottom': '2em'}),
    dbc.Row([
        dbc.Col([
            html.Br(),

            html.H3("საჯარიმო ქულები,სიჩქარის გადაჭარბება და გარბენი მძღოლების მიხედვით", className='text-center text-primary mb-4'),
            html.Br(),
            dash_table.DataTable(
                id='aggregate_drivers_table2',
                columns=[
                    {'id': "region", 'name': "region"},
                    {'id': "service_center", 'name': "service_center"},
                    {'id': "driver", 'name': "driver"},
                    dict(id='milage', name='milage', type='numeric', format=Format(precision=2, scheme=Scheme.fixed)),
                    {'id': "max_speed", 'name': "max_speed"},
                    {'id': "speed_exc_cnt", 'name': "speed_limit_exceed"},
                    {'id': "harsh_a_points", 'name': "Harsh Acceleration points"},
                    {'id': "harsh_b_points", 'name': "Harsh Breaking points"},
                    {'id': "harsh_c_points", 'name': "Harsh Cornering points"},
                    {'id': "v_harsh_a_points", 'name': "Very Harsh Acceleration points"},
                    {'id': "v_harsh_b_points", 'name': "Very Harsh Breaking points"},
                    {'id': "v_harsh_c_points", 'name': "Very Harsh Cornering points"},
                    {'id': "total_penalty_points", 'name': "total penalty points"}
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
                #style_table={'height': '500px', 'overflowY': 'auto'},
                style_table={'overflowX': 'auto'},
                style_cell={
                    'height': 'auto',
                    # all three widths are needed
                    'minWidth': '100px', 'width': '100px', 'maxWidth': '180px',
                    'whiteSpace': 'normal'
                },
                page_size=200,  # number of rows visible per page
                #style_cell={  # ensure adequate header width when text is shorter than cell's text
                #    'minWidth': 95, 'maxWidth': 130, 'width': 105
                #},
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
            )], width={'size': 12, 'offset': 0}),



        html.Div(id='placeholder2', children=[]),
        dcc.Interval(id='interval2', interval=1000)
    ], no_gutters=False, justify='left', style={'marginBottom': '2em'}),
    html.Br(),
    # html.Button("Download Excel", id="btn_xlsx"),
    # dcc.Download(id="download-dataframe-xlsx"),


], fluid=True)



@app.callback(
    Output(component_id='aggregate_drivers_table2', component_property='data'),
    #Input(component_id='region_dropdown2', component_property='value'),
    #Input(component_id='sc_dropdown2', component_property='value'),
    Input('my-date-picker-range2', 'start_date'),
    Input('my-date-picker-range2', 'end_date'),
    Input('interval_pg2', 'n_intervals')
)
def update_aggregate_drv_rows(start, end, n_intervals):
    data_group = read_data(start, end)
    return data_group.to_dict('records')

# @app.callback(
#     Output("download-dataframe-xlsx", "data"),
#     Input("btn_xlsx", "n_clicks"),
#     Input('my-date-picker-range2', 'start_date'),
#     Input('my-date-picker-range2', 'end_date'),
#     #Input('interval_pg2', 'n_intervals'),
#     prevent_initial_call=True,
# )
# def func(n_clicks,start, end):
#     df = read_data(start, end)
#     return dcc.send_data_frame(df.to_excel, "mydf.xlsx", sheet_name="Sheet_name_1")


# @app.callback(
#     Output("download-dataframe-xlsx", "data"),
#     Input("btn_xlsx", "n_clicks"),
#     prevent_initial_call=True,
# )
# def func(n_clicks):
#     return dcc.send_data_frame(df.to_excel, "mydf.xlsx", sheet_name="Sheet_name_1")