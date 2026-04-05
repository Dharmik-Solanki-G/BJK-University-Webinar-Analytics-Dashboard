"""
app.py — BJK University Webinar Analytics Dashboard
Main Dash application: layout, charts, callbacks.
"""
import dash
from dash import html, dcc, dash_table, callback, Output, Input, State, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

from data_loader import load_all
from data_pipeline import build_master_dataframe
from metrics import compute_all

# ── Load & Process Data ───────────────────────────────────────
data, metadata = load_all()
master = build_master_dataframe(data, metadata)
M = compute_all(master, metadata)

# ── Theme Colors ──────────────────────────────────────────────
BG = '#0A0A10'
CARD = '#13131E'
GOLD = '#F59E0B'
BLUE = '#3B82F6'
GREEN = '#22C55E'
GRAY = '#6B7280'
TEXT = '#F0F0FC'
MUTED = '#8888A8'
GRID = '#1E1E30'
SRC_COLORS = {'Organic': GREEN, 'Paid': GOLD}

PLOT_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='Inter, sans-serif', color=TEXT, size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    yaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(size=11)),
    hoverlabel=dict(bgcolor=CARD, font=dict(color=TEXT), namelength=0, bordercolor=GRID),
)


def stat_card(label, value, color, sub=''):
    return html.Div(className='stat-card', children=[
        html.Div(style={'backgroundColor': color}, className='accent-bar'),
        html.Div(label, className='label'),
        html.Div(str(value), className='value', style={'color': color}),
        html.Div(sub, className='sub') if sub else None,
    ])


def mask_email(e):
    if not isinstance(e, str) or '@' not in e:
        return e
    local, domain = e.split('@', 1)
    return f"{local[0]}***@{domain}" if local else e


# ── Charts ────────────────────────────────────────────────────
def fig_funnel_bars():
    """Grouped bar: Registrations vs Attendance by Source."""
    src = M['source']
    sources = ['Organic', 'Paid']
    regs = [src[s]['registered'] for s in sources]
    atts = [src[s]['attended'] for s in sources]
    rates = [f"{src[s]['show_rate']}%" for s in sources]
    colors_reg = ['rgba(34,197,94,0.3)', 'rgba(245,158,11,0.3)']
    colors_att = [GREEN, GOLD]
    fig = go.Figure()
    fig.add_trace(go.Bar(name='Registered', x=sources, y=regs, marker_color=colors_reg,
                         marker_line=dict(width=1, color=colors_att), text=regs, textposition='outside',
                         textfont=dict(size=13, color=TEXT)))
    fig.add_trace(go.Bar(name='Attended', x=sources, y=atts, marker_color=colors_att,
                         text=[f"{a} ({r})" for a, r in zip(atts, rates)], textposition='outside',
                         textfont=dict(size=12, color=TEXT)))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(barmode='group', title=None,
                      yaxis_title='People', showlegend=True,
                      legend=dict(orientation='h', y=-0.15, x=0.5, xanchor='center'))
    fig.update_yaxes(range=[0, max(regs) * 1.25])
    return fig


def fig_reg_donut():
    """Donut: Registration split by source."""
    src = M['source']
    labels = ['Organic', 'Paid']
    values = [src[s]['registered'] for s in labels]
    colors = [GREEN, GOLD]
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.65,
        marker=dict(colors=colors, line=dict(color=BG, width=2)),
        textinfo='label+value', textposition='outside',
        textfont=dict(size=12, color=TEXT),
        hoverinfo='label+value+percent',
    ))
    fig.update_layout(**PLOT_LAYOUT)
    fig.add_annotation(text=f"<b>{M['funnel']['total_registered']}</b><br><span style='font-size:11px;color:{MUTED}'>Total</span>",
                       showarrow=False, font=dict(size=28, color=TEXT), x=0.5, y=0.5)
    return fig


def fig_showup_bars():
    """Horizontal bars: Show-up rate by source."""
    src = M['source']
    sources = ['Paid', 'Organic']
    rates = [src[s]['show_rate'] for s in sources]
    colors = [SRC_COLORS[s] for s in sources]
    fig = go.Figure(go.Bar(
        x=rates, y=sources, orientation='h', marker_color=colors,
        text=[f"{r}%" for r in rates], textposition='outside',
        textfont=dict(size=14, color=TEXT, family='Inter'),
    ))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(title=None, xaxis_title='Show-Up Rate %',
                      xaxis=dict(range=[0, 100], gridcolor=GRID),
                      yaxis=dict(gridcolor=GRID), height=300)
    return fig


def fig_noshow():
    """Stacked bar: Attended vs No-Show by source."""
    src = M['source']
    sources = ['Organic', 'Paid']
    attended = [src[s]['attended'] for s in sources]
    noshow = [src[s]['no_show'] for s in sources]
    fig = go.Figure()
    fig.add_trace(go.Bar(name='Attended', x=sources, y=attended,
                         marker_color=[GREEN, GOLD],
                         text=attended, textposition='inside', textfont=dict(size=12, color='#000')))
    fig.add_trace(go.Bar(name='No-Show', x=sources, y=noshow,
                         marker_color=['rgba(34,197,94,0.25)', 'rgba(245,158,11,0.25)'],
                         text=noshow, textposition='inside', textfont=dict(size=12, color=TEXT)))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(barmode='stack', title=None,
                      legend=dict(orientation='h', y=-0.15, x=0.5, xanchor='center'), height=300)
    return fig


def fig_engagement():
    """Grouped bar: Engagement distribution by source."""
    eng = M['engagement']
    if eng.empty:
        return go.Figure().update_layout(**PLOT_LAYOUT)
    fig = go.Figure()
    for src, color in [('Organic', GREEN), ('Paid', GOLD)]:
        if src in eng.columns:
            fig.add_trace(go.Bar(name=src, x=eng['band'], y=eng[src],
                                 marker_color=color, text=eng[src], textposition='outside',
                                 textfont=dict(size=11, color=TEXT)))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(barmode='group', title=None,
                      xaxis_title='Engagement Band', yaxis_title='Attendees',
                      legend=dict(orientation='h', y=-0.2, x=0.5, xanchor='center'))
    return fig


def fig_timeline():
    """Area chart: Registration timeline by source."""
    tl = M['timeline']
    if tl.empty:
        return go.Figure().update_layout(**PLOT_LAYOUT)
    fig = go.Figure()
    for src, color in [('Organic', GREEN), ('Paid', GOLD)]:
        if src in tl.columns:
            fig.add_trace(go.Scatter(
                x=tl['reg_date'], y=tl[src], name=src, mode='lines',
                line=dict(color=color, width=2),
                fill='tozeroy', fillcolor=color.replace(')', ',0.1)').replace('rgb', 'rgba') if 'rgb' in color else f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.1)",
            ))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(title=None,
                      xaxis_title='Registration Date', yaxis_title='Registrations',
                      legend=dict(orientation='h', y=-0.2, x=0.5, xanchor='center'),
                      hovermode='x unified')
    fig.update_xaxes(tickformat='%b %d')
    return fig


# ── Dash App ──────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}],
    title='BJK University — Webinar Analytics',
)
server = app.server  # Required for gunicorn/Render

import flask
import os
from data_loader import DATA_DIR
@app.server.route('/screenshots/<path:path>')
def serve_screenshots(path):
    bc_dir = os.path.join(DATA_DIR, 'Booked Calls')
    return flask.send_from_directory(bc_dir, path)

# Prepare table data
table_df = master[['name', 'email', 'source', 'attended', 'duration_minutes', 'engagement_pct', 'ad_platform', 'booked_call']].copy()
table_df['email_masked'] = table_df['email'].apply(mask_email)
table_df['attended_label'] = table_df['attended'].map({True: '✅ YES', False: '❌ NO'})
table_df['duration_minutes'] = table_df['duration_minutes'].round(1)
table_df['booked_label'] = table_df['booked_call'].map({True: '📞 YES', False: ''})
table_df['screenshot_button'] = table_df['booked_call'].map({True: '📷 View', False: ''})
table_df['screenshot_filename'] = master['screenshot_filename']

webinar_duration = M['webinar_duration']
def format_engagement(row):
    if row['duration_minutes'] > webinar_duration:
        return "100%+"
    return f"{row['engagement_pct']}%"
table_df['engagement_display'] = table_df.apply(format_engagement, axis=1)

f = M['funnel']
s = M['source']
b = M['booked']
now = datetime.now().strftime('%B %d, %Y at %I:%M %p')

paid_sr = s['Paid']['show_rate']
org_sr = s['Organic']['show_rate']
diff_pct = round((paid_sr - org_sr) / org_sr * 100) if org_sr > 0 else 0
direction = "more likely" if diff_pct > 0 else "less likely"
insight_text = (
    f"Paid traffic drove {s['Paid']['registered']} registrations "
    f"({round(s['Paid']['registered']/f['total_registered']*100)}% of total) "
    f"but delivered a {paid_sr}% show rate vs {org_sr}% for organic — "
    f"meaning paid leads are ~{abs(diff_pct)}% {direction} to show up. "
    f"Of the {f['total_attended']} who attended, "
    f"{b['total_booked']} booked a call ({b['booking_rate']}% conversion)."
)

app.layout = html.Div(style={'backgroundColor': BG, 'minHeight': '100vh'}, children=[

    # ── HEADER ────────────────────────────────────────────────
    html.Div(className='header-bar', children=[
        html.Div(className='header-left', children=[
            html.H1(['BJK ', html.Span('University')]),
            html.P('Webinar Performance Analytics Dashboard'),
        ]),
        html.Div(className='header-right', children=[
            html.Span('Wednesday, April 1, 2026'),
            html.Span('|'),
            html.Span(f'Generated {now}'),
            html.Span('|'),
            html.Span(className='live-dot'),
            html.Span('LIVE', style={'color': GREEN, 'fontWeight': 600}),
        ]),
    ]),

    # ── MAIN CONTENT ──────────────────────────────────────────
    html.Div(style={'padding': '24px 32px', 'maxWidth': '1440px', 'margin': '0 auto'}, children=[

        # Section: Key Metrics
        html.Div('Key Performance Indicators', className='section-label'),
        dbc.Row([
            dbc.Col(stat_card('Total Registered', f"{f['total_registered']:,}", TEXT,
                              f"944 webinar signups"), md=True),
            dbc.Col(stat_card('Total Attended', f"{f['total_attended']:,}", GOLD,
                              f"{f['total_no_show']:,} no-shows"), md=True),
            dbc.Col(stat_card('Show-Up Rate', f"{f['show_rate']}%", 
                              GREEN if f['show_rate'] > 50 else GOLD if f['show_rate'] > 30 else '#EF4444',
                              f"of all registrants"), md=True),
            dbc.Col(stat_card('Organic Show Rate', f"{s['Organic']['show_rate']}%", GREEN,
                              f"{s['Organic']['attended']} of {s['Organic']['registered']}"), md=True),
            dbc.Col(stat_card('Paid Show Rate', f"{s['Paid']['show_rate']}%", GOLD,
                              f"{s['Paid']['attended']} of {s['Paid']['registered']}"), md=True),
        ], className='g-3 mb-3'),

        # Row 2: Extra stat cards
        dbc.Row([
            dbc.Col(stat_card('Booked Calls', f"{b['total_booked']}", BLUE,
                              f"{b['booking_rate']}% of attendees"), md=3),
            dbc.Col(stat_card('Avg Duration (Organic)', f"{s['Organic']['avg_duration']} min", GREEN,
                              f"{s['Organic']['avg_engagement']}% engagement"), md=3),
            dbc.Col(stat_card('Avg Duration (Paid)', f"{s['Paid']['avg_duration']} min", GOLD,
                              f"{s['Paid']['avg_engagement']}% engagement"), md=3),
            dbc.Col(stat_card('Peak Concurrent', f"{M['max_concurrent']}", BLUE,
                              f"per Zoom report — {M['webinar_duration']} min runtime"), md=3),
        ], className='g-3 mb-4'),

        html.Div(className='insight-bar', children=[
            html.Span("KEY INSIGHT  ", style={
                'color': GOLD,
                'fontWeight': 700,
                'fontSize': '11px'
            }),
            html.Span(insight_text, style={
                'color': TEXT,
                'fontSize': '13px'
            }),
        ]),

        # Section: Funnel Analysis
        html.Div('Funnel Analysis', className='section-label'),
        dbc.Row([
            dbc.Col(html.Div(className='chart-card', children=[
                html.H3(['Registrations vs Attendance ', html.Span('by Source')]),
                dcc.Graph(figure=fig_funnel_bars(), config={'displayModeBar': False},
                          style={'height': '380px'}),
            ]), md=7),
            dbc.Col(html.Div(className='chart-card', children=[
                html.H3(['Registration Split ', html.Span('by Source')]),
                dcc.Graph(figure=fig_reg_donut(), config={'displayModeBar': False},
                          style={'height': '380px'}),
            ]), md=5),
        ], className='g-3 mb-4'),

        # Section: Show-Up Analysis
        html.Div('Show-Up Rate Analysis', className='section-label'),
        dbc.Row([
            dbc.Col(html.Div(className='chart-card', children=[
                html.H3(['Show-Up Rate ', html.Span('by Source')]),
                dcc.Graph(figure=fig_showup_bars(), config={'displayModeBar': False},
                          style={'height': '300px'}),
            ]), md=6),
            dbc.Col(html.Div(className='chart-card', children=[
                html.H3(['No-Show Analysis ', html.Span('Attended vs Lost')]),
                dcc.Graph(figure=fig_noshow(), config={'displayModeBar': False},
                          style={'height': '300px'}),
            ]), md=6),
        ], className='g-3 mb-4'),

        # Section: Engagement
        html.Div('Engagement Depth', className='section-label'),
        dbc.Row([
            dbc.Col(html.Div(className='chart-card', children=[
                html.H3(['How Long Did Attendees Stay? ', html.Span('Engagement Distribution')]),
                dcc.Graph(figure=fig_engagement(), config={'displayModeBar': False},
                          style={'height': '360px'}),
            ]), md=6),
            dbc.Col(html.Div(className='chart-card', children=[
                html.H3(['Registration Timeline ', html.Span('Organic vs Paid Velocity')]),
                dcc.Graph(figure=fig_timeline(), config={'displayModeBar': False},
                          style={'height': '360px'}),
            ]), md=6),
        ], className='g-3 mb-4'),

        # Section: Data Table
        html.Div('Detailed Registrant Data', className='section-label'),
        html.Div(className='chart-card', children=[
            html.H3(['All Registrants ', html.Span(f'{len(table_df)} records')]),
            dash_table.DataTable(
                id='main-table',
                columns=[
                    {'name': 'Name', 'id': 'name'},
                    {'name': 'Email', 'id': 'email_masked'},
                    {'name': 'Source', 'id': 'source'},
                    {'name': 'Attended', 'id': 'attended_label'},
                    {'name': 'Duration (min)', 'id': 'duration_minutes', 'type': 'numeric'},
                    {'name': 'Engagement %', 'id': 'engagement_display'},
                    {'name': 'Platform', 'id': 'ad_platform'},
                    {'name': 'Booked Call', 'id': 'booked_label'},
                    {'name': 'Pictures', 'id': 'screenshot_button'},
                ],
                data=table_df.sort_values('duration_minutes', ascending=False).to_dict('records'),
                page_size=20,
                sort_action='native',
                filter_action='native',
                export_format='csv',
                style_table={'overflowX': 'auto'},
                style_header={
                    'backgroundColor': '#1A1A2E', 'color': MUTED,
                    'fontWeight': '600', 'fontSize': '11px', 'textTransform': 'uppercase',
                    'letterSpacing': '0.8px', 'borderBottom': f'2px solid {GOLD}',
                    'padding': '12px 16px',
                },
                style_cell={
                    'backgroundColor': CARD, 'color': TEXT,
                    'fontSize': '13px', 'padding': '10px 16px',
                    'borderBottom': f'1px solid {GRID}', 'textAlign': 'left',
                    'fontFamily': 'Inter, sans-serif', 'minWidth': '100px',
                },
                style_data_conditional=[
                    {'if': {'filter_query': '{attended_label} contains "YES"'},
                     'backgroundColor': 'rgba(34,197,94,0.08)'},
                    {'if': {'filter_query': '{source} = "Organic"', 'column_id': 'source'},
                     'color': GREEN, 'fontWeight': '600'},
                    {'if': {'filter_query': '{source} = "Paid"', 'column_id': 'source'},
                     'color': GOLD, 'fontWeight': '600'},
                    {'if': {'column_id': 'screenshot_button', 'filter_query': '{screenshot_button} != ""'},
                     'color': '#3B82F6', 'cursor': 'pointer', 'textDecoration': 'underline', 'fontWeight': 'bold'},
                ],
                style_filter={
                    'backgroundColor': '#1A1A2E', 'color': TEXT,
                    'borderBottom': f'1px solid {GRID}',
                },
            ),
        ]),
    ]),

    # ── FOOTER ────────────────────────────────────────────────
    html.Div(className='footer-bar', children=[
        f'Dashboard built for BJK University  |  Data: Zoom Webinar Reports + ClickFunnels  |  {now}',
    ]),

    # ── MODALS ────────────────────────────────────────────────
    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Booking Confirmation Screenshot"), close_button=True),
            dbc.ModalBody(
                html.Img(id='screenshot-img', style={'width': '100%', 'borderRadius': '4px'}),
                style={'backgroundColor': '#13131E', 'padding': '16px'}
            ),
        ],
        id='screenshot-modal',
        size='xl',
        centered=True,
        is_open=False,
    )
])

@callback(
    Output('screenshot-modal', 'is_open'),
    Output('screenshot-img', 'src'),
    Input('main-table', 'active_cell'),
    State('main-table', 'derived_virtual_data'),
    prevent_initial_call=True
)
def display_screenshot(active_cell, data):
    if active_cell and active_cell['column_id'] == 'screenshot_button' and data:
        row_data = data[active_cell['row']]
        if row_data.get('screenshot_filename'):
            return True, f"/screenshots/{row_data['screenshot_filename']}"
    return no_update, no_update

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)
