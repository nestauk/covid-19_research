import plotly.express as px
import cord19
import altair as alt
import pandas as pd
import plotly.express as px


def scatter_3d(embed, data):
    """Creates a 3d scatter plot and stores it as HTML.

    Args:
        embed (:obj:`list` of `numpy.array`): Vectors.
        data (`pandas.DataFrame`): Dataframe with a `title` and `is_Covid` columns.

    """
    df = pd.DataFrame(embed)
    df["title"] = [t for t in data["title"]]
    df["is_Covid"] = ["is_covid" if i == 1 else "not_covid" for i in data["is_Covid"]]

    fig = px.scatter_3d(df, x=0, y=1, z=2, hover_name="title", color="is_Covid")
    fig.update_traces(
        marker=dict(size=2, line=dict(width=2, color="DarkSlateGrey")),
        selector=dict(mode="markers"),
    )
    fig.write_html(f"{cord19.project_dir}/reports/figures/umap_viz_ai_covid.html")


def bar_chart(data, x_col, y_col, title):
    """Creates a bar chart.

    Args:
        data (`pandas.DataFrame`)
        x_col (str): Column name that will be used in x axis.
        y_col (str): Column name that will be used in y axis.
        title (str): Title of the figure.

    Returns:
        Altair bar chart.

    """
    return (
        alt.Chart(data)
        .mark_bar()
        .encode(x=alt.X(x_col, sort="-y"), y=alt.Y(y_col))
        .properties(width=1200, title=title)
    )
