"""
Phase 3 — Books Data Dashboard
Interactive Dash dashboard for books_transformed.csv
Run: python dashboard.py  →  open http://127.0.0.1:8050
"""

import pandas as pd
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go

# ── Load data ─────────────────────────────────────────────────────────────────

CSV_PATH = r"new_data\transformed\books\books_transformed.csv"
df = pd.read_csv(CSV_PATH)

# ── App init ──────────────────────────────────────────────────────────────────

app = dash.Dash(__name__)
app.title = "Books Dashboard"

# ── Colour palette ────────────────────────────────────────────────────────────

PRIMARY   = "#4361ee"
SECONDARY = "#3f37c9"
ACCENT    = "#f72585"
BG        = "#f8f9fa"
CARD_BG   = "#ffffff"
TEXT      = "#212529"
MUTED     = "#6c757d"

RATING_COLORS = {
    1: "#ef233c",
    2: "#fb8500",
    3: "#ffb703",
    4: "#80b918",
    5: "#2dc653",
}

# ── KPI helpers ───────────────────────────────────────────────────────────────

def kpi_card(label, value, color=PRIMARY):
    return html.Div([
        html.P(label, style={"margin": 0, "fontSize": "13px", "color": MUTED, "fontWeight": "600", "letterSpacing": "0.05em", "textTransform": "uppercase"}),
        html.H3(value, style={"margin": "4px 0 0 0", "fontSize": "28px", "color": color, "fontWeight": "800"}),
    ], style={
        "background": CARD_BG,
        "borderRadius": "12px",
        "padding": "20px 28px",
        "boxShadow": "0 2px 12px rgba(0,0,0,0.07)",
        "flex": "1",
        "minWidth": "160px",
    })

# ── Layout ────────────────────────────────────────────────────────────────────

app.layout = html.Div(style={"backgroundColor": BG, "minHeight": "100vh", "fontFamily": "'Segoe UI', sans-serif", "color": TEXT}, children=[

    # ── Header ────────────────────────────────────────────────────────────────
    html.Div([
        html.H1("📚 Books Dashboard", style={"margin": 0, "fontSize": "26px", "fontWeight": "800", "color": "white"}),
        html.P("books.toscrape.com · 1,000 titles · Phase 3 Analysis",
               style={"margin": "4px 0 0 0", "fontSize": "13px", "color": "rgba(255,255,255,0.75)"}),
    ], style={
        "background": f"linear-gradient(135deg, {PRIMARY}, {SECONDARY})",
        "padding": "24px 40px",
        "boxShadow": "0 4px 20px rgba(67,97,238,0.3)",
    }),

    # ── Filters ───────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.Label("Filter by Rating", style={"fontWeight": "600", "fontSize": "13px", "color": MUTED}),
            dcc.Checklist(
                id="filter-rating",
                options=[{"label": f"  {'★' * r} ({r})", "value": r} for r in [1, 2, 3, 4, 5]],
                value=[1, 2, 3, 4, 5],
                inline=True,
                style={"marginTop": "6px", "fontSize": "14px"},
                inputStyle={"marginRight": "4px", "accentColor": PRIMARY},
                labelStyle={"marginRight": "16px"},
            ),
        ], style={"flex": "2"}),

        html.Div([
            html.Label("Price Range (£)", style={"fontWeight": "600", "fontSize": "13px", "color": MUTED}),
            dcc.RangeSlider(
                id="filter-price",
                min=df["price"].min(), max=df["price"].max(),
                step=1,
                value=[df["price"].min(), df["price"].max()],
                marks={int(v): f"£{int(v)}" for v in [10, 20, 30, 40, 50, 60]},
                tooltip={"placement": "bottom", "always_visible": False},
            ),
        ], style={"flex": "3", "paddingTop": "4px"}),

        html.Div([
            html.Label("Availability", style={"fontWeight": "600", "fontSize": "13px", "color": MUTED}),
            dcc.RadioItems(
                id="filter-availability",
                options=[
                    {"label": "  All",      "value": "all"},
                    {"label": "  In Stock", "value": 1},
                    {"label": "  Out",      "value": 0},
                ],
                value="all",
                inline=True,
                style={"marginTop": "6px", "fontSize": "14px"},
                inputStyle={"marginRight": "4px", "accentColor": PRIMARY},
                labelStyle={"marginRight": "14px"},
            ),
        ], style={"flex": "1.5"}),

    ], style={
        "display": "flex", "gap": "32px", "alignItems": "flex-start",
        "flexWrap": "wrap",
        "background": CARD_BG,
        "padding": "20px 40px",
        "boxShadow": "0 2px 8px rgba(0,0,0,0.05)",
        "borderBottom": f"3px solid {PRIMARY}",
    }),

    # ── KPI row ───────────────────────────────────────────────────────────────
    html.Div(id="kpi-row", style={
        "display": "flex", "gap": "16px", "flexWrap": "wrap",
        "padding": "24px 40px 8px 40px",
    }),

    # ── Charts grid ───────────────────────────────────────────────────────────
    html.Div([

        # Row 1
        html.Div([
            html.Div([dcc.Graph(id="chart-price-dist", config={"displayModeBar": False})],
                     style={"flex": "1", "background": CARD_BG, "borderRadius": "12px", "padding": "16px", "boxShadow": "0 2px 12px rgba(0,0,0,0.06)"}),
            html.Div([dcc.Graph(id="chart-rating-dist", config={"displayModeBar": False})],
                     style={"flex": "1", "background": CARD_BG, "borderRadius": "12px", "padding": "16px", "boxShadow": "0 2px 12px rgba(0,0,0,0.06)"}),
        ], style={"display": "flex", "gap": "20px", "marginBottom": "20px"}),

        # Row 2
        html.Div([
            html.Div([dcc.Graph(id="chart-availability", config={"displayModeBar": False})],
                     style={"flex": "1", "background": CARD_BG, "borderRadius": "12px", "padding": "16px", "boxShadow": "0 2px 12px rgba(0,0,0,0.06)"}),
            html.Div([dcc.Graph(id="chart-price-rating", config={"displayModeBar": False})],
                     style={"flex": "2", "background": CARD_BG, "borderRadius": "12px", "padding": "16px", "boxShadow": "0 2px 12px rgba(0,0,0,0.06)"}),
        ], style={"display": "flex", "gap": "20px", "marginBottom": "20px"}),

        # Row 3
        html.Div([
            html.Div([dcc.Graph(id="chart-top-books", config={"displayModeBar": False})],
                     style={"flex": "1", "background": CARD_BG, "borderRadius": "12px", "padding": "16px", "boxShadow": "0 2px 12px rgba(0,0,0,0.06)"}),
        ], style={"display": "flex", "gap": "20px"}),

    ], style={"padding": "20px 40px 40px 40px"}),

])

# ── Callback ──────────────────────────────────────────────────────────────────

@app.callback(
    Output("kpi-row",          "children"),
    Output("chart-price-dist", "figure"),
    Output("chart-rating-dist","figure"),
    Output("chart-availability","figure"),
    Output("chart-price-rating","figure"),
    Output("chart-top-books",  "figure"),
    Input("filter-rating",       "value"),
    Input("filter-price",        "value"),
    Input("filter-availability", "value"),
)
def update(ratings, price_range, avail):

    # ── Filter ────────────────────────────────────────────────────────────────
    mask = (
        df["rating"].isin(ratings) &
        df["price"].between(price_range[0], price_range[1])
    )
    if avail != "all":
        mask &= df["availability"] == avail
    d = df[mask]

    # ── KPIs ──────────────────────────────────────────────────────────────────
    kpis = html.Div([
        kpi_card("Total Books",    f"{len(d):,}"),
        kpi_card("Avg Price",      f"£{d['price'].mean():.2f}"  if len(d) else "—", SECONDARY),
        kpi_card("Avg Rating",     f"{d['rating'].mean():.2f} ★" if len(d) else "—", "#f72585"),
        kpi_card("In Stock",       f"{d['availability'].sum():,}", "#2dc653"),
        kpi_card("Most Common Rating", f"{'★' * int(d['rating'].mode()[0]) if len(d) else '—'}", "#fb8500"),
        kpi_card("Price Range",    f"£{d['price'].min():.0f} – £{d['price'].max():.0f}" if len(d) else "—", MUTED),
    ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "width": "100%"})

    chart_layout = dict(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="Segoe UI, sans-serif", color=TEXT),
        margin=dict(t=40, b=30, l=30, r=20),
    )

    # ── 1. Price distribution ─────────────────────────────────────────────────
    fig_price = px.histogram(
        d, x="price", nbins=30,
        title="Price Distribution",
        labels={"price": "Price (£)", "count": "Books"},
        color_discrete_sequence=[PRIMARY],
    )
    fig_price.update_traces(marker_line_width=0.5, marker_line_color="white")
    fig_price.update_layout(**chart_layout, bargap=0.05)

    # ── 2. Rating distribution ────────────────────────────────────────────────
    rating_counts = d["rating"].value_counts().sort_index().reset_index()
    rating_counts.columns = ["rating", "count"]
    rating_counts["color"] = rating_counts["rating"].map(RATING_COLORS)
    rating_counts["label"] = rating_counts["rating"].apply(lambda r: f"{'★'*r}{'☆'*(5-r)}")

    fig_rating = go.Figure(go.Bar(
        x=rating_counts["label"],
        y=rating_counts["count"],
        marker_color=rating_counts["color"],
        text=rating_counts["count"],
        textposition="outside",
    ))
    fig_rating.update_layout(title="Rating Distribution", xaxis_title="Rating", yaxis_title="Books", **chart_layout)

    # ── 3. Availability donut ─────────────────────────────────────────────────
    in_stock  = int(d["availability"].sum())
    out_stock = len(d) - in_stock
    fig_avail = go.Figure(go.Pie(
        labels=["In Stock", "Out of Stock"],
        values=[in_stock, out_stock],
        hole=0.55,
        marker_colors=["#2dc653", "#ef233c"],
        textinfo="label+percent",
    ))
    fig_avail.update_layout(title="Stock Availability", **chart_layout,
                            annotations=[dict(text=f"{in_stock}<br>in stock", x=0.5, y=0.5,
                                             font_size=14, showarrow=False)])

    # ── 4. Price by rating box plot ───────────────────────────────────────────
    fig_box = px.box(
        d, x="rating", y="price",
        title="Price Distribution by Rating",
        labels={"rating": "Rating (stars)", "price": "Price (£)"},
        color="rating",
        color_discrete_map=RATING_COLORS,
    )
    fig_box.update_layout(**chart_layout, showlegend=False)

    # ── 5. Top 15 most expensive books ────────────────────────────────────────
    top = d.nlargest(15, "price")[["title", "price", "rating"]].copy()
    top["short_title"] = top["title"].apply(lambda t: t[:40] + "…" if len(t) > 40 else t)
    top["color"] = top["rating"].map(RATING_COLORS)

    fig_top = go.Figure(go.Bar(
        x=top["price"],
        y=top["short_title"],
        orientation="h",
        marker_color=top["color"],
        text=top["price"].apply(lambda p: f"£{p:.2f}"),
        textposition="outside",
        customdata=top[["title", "rating"]].values,
        hovertemplate="<b>%{customdata[0]}</b><br>Price: £%{x:.2f}<br>Rating: %{customdata[1]}★<extra></extra>",
    ))
    fig_top.update_layout(
        title="Top 15 Most Expensive Books (coloured by rating)",
        xaxis_title="Price (£)", yaxis_title="",
        **chart_layout,
        height=480,
        yaxis=dict(autorange="reversed"),
    )

    return kpis, fig_price, fig_rating, fig_avail, fig_box, fig_top


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Dashboard running at http://127.0.0.1:8050", flush=True)
    app.run(debug=False)