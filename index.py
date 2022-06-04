import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

# Connect to main app.py file
from app import app
from app import server

# Connect to your app pages
from apps import drivers,skip_trace #,main,service,cars


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([
        dcc.Link(' აგრეგაციები | ', href='/apps/drivers'),
        dcc.Link(' დეტალური მონაცემები | ', href='/apps/main'),
        dcc.Link(' სასერვისო ოდომეტრის ჩვენებები |', href='/apps/cars'),
        dcc.Link(' სერვის რეპორტი |', href='/apps/service'),
        dcc.Link(' სქიპ ტრეისი |', href='/apps/skip_trace'),
    ], className="row"),
    html.Div(id='page-content', children=[])
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/main':
        return main.layout
    if pathname == '/apps/service':
        return service.layout
    if pathname == '/apps/cars':
        return cars.layout
    if pathname == '/apps/skip_trace':
        return skip_trace.layout

    else:
        return drivers.layout


if __name__ == '__main__':
    app.run_server(debug=False)