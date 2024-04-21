from configparser import ConfigParser
from datetime import datetime as dt
import pandas as pd
import psycopg2
import psycopg2.extras
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go

# Function to load database connection parameters
def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db_params = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db_params[item[0]] = item[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db_params

# Function to connect to the PostgreSQL database server
def fetch_data(start_date, end_date):
    params = load_config()
    conn = psycopg2.connect(**params)
    
    # Make sure the dates are Unix timestamps in milliseconds, as bigint
    start_timestamp = int(start_date.timestamp() * 1000)
    end_timestamp = int(end_date.timestamp() * 1000)
    
    query = """
    SELECT
        timestamp, open, high, low, close, volume
    FROM
        "BTCUSDT:USDT"
    WHERE
        timestamp >= %s AND timestamp <= %s
    """
    
    df = pd.read_sql(query, conn, params=(start_timestamp, end_timestamp))
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    return df

# Function to resample data according to the selected timeframe
def resample_data(df, timeframe):
    return df.resample(timeframe).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })

# Dash app layout and callback functions
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Cryptocurrency Data Visualization"),
    dcc.DatePickerRange(
        id='date-picker-range',  # Ensure this ID matches the one used in the callback
        start_date=dt.today().date() - pd.Timedelta(days=1),
        end_date=dt.today().date(),
        display_format='YYYY-MM-DD',
        max_date_allowed=dt.today().date()
    ),
    dcc.RadioItems(
        id='timeframe-selector',
        options=[
            {'label': '1 Minute', 'value': '1T'},
            {'label': '5 Minutes', 'value': '5T'},
            {'label': '10 Minutes', 'value': '10T'},
            {'label': '1 Hour', 'value': '1H'},
            {'label': '1 Day', 'value': '1D'}
        ],
        value='1T',
        labelStyle={'display': 'inline-block'}
    ),
    dcc.Graph(id='crypto-chart'),
    dcc.Loading(id="loading-icon", children=[html.Div(dcc.Graph(id='crypto-chart'))], type="circle"),
])

@app.callback(
    Output('crypto-chart', 'figure'),
    [Input('timeframe-selector', 'value')],
    [State('date-picker-range', 'start_date'),
     State('date-picker-range', 'end_date')]  # These should match the IDs in the layout
)
def update_chart(selected_timeframe, start_date, end_date):
    start_date_obj = dt.strptime(start_date, '%Y-%m-%d')
    end_date_obj = dt.strptime(end_date, '%Y-%m-%d')
    df = fetch_data(start_date_obj, end_date_obj)
    resampled_df = resample_data(df, selected_timeframe)
    
    fig = go.Figure()

    # Update traces
    fig.add_trace(go.Candlestick(
        x=resampled_df.index,
        open=resampled_df['open'],
        high=resampled_df['high'],
        low=resampled_df['low'],
        close=resampled_df['close'],
        name="Candlesticks"
    ))

    # Update layout for a full-screen responsive graph
    fig.update_layout(
        autosize=True,
        xaxis=dict(
            autorange=True,
            rangeslider=dict(
                visible=True,
                autorange=False,
                thickness=0.1
            ),
            type='date',
            fixedrange=False  # Allows horizontal scroll
        ),
        yaxis=dict(
            autorange=True,
            fixedrange=True  # Prevents vertical scroll
        ),
        margin=dict(l=0, r=0, t=0, b=0),  # Reduces default margins to minimal
        dragmode='pan'  # Allows panning as default interaction
    )

    # Set x-axis range limits to prevent excessive zooming
    # For example, prevent zooming in more than 1 day worth of data
    min_zoom_range = pd.Timedelta(days=1).total_seconds() * 1000
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=[min_zoom_range, 'T1'])  # min_zoom_range milliseconds per interval
        ]
    )
    
    # Use full screen upon page load and resize
    fig.update_layout(height=1000)  # Adjust height to fit the screen or as required

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
