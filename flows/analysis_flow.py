from prefect import flow, task
import duckdb
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DB_PATH = "data/db/reaction.duckdb"
analyzer = SentimentIntensityAnalyzer()


@task
def add_sentiment():
    conn = duckdb.connect(DB_PATH)

    df = conn.execute(
        """
        SELECT rowid, text
        FROM social_events
        WHERE sentiment IS NULL
        """
    ).df()

    for _, row in df.iterrows():
        text = row["text"] or ""
        score = analyzer.polarity_scores(text)["compound"]
        conn.execute(
            "UPDATE social_events SET sentiment = ? WHERE rowid = ?",
            (score, row["rowid"]),
        )

    conn.close()


@flow
def analysis_flow():
    add_sentiment()


if __name__ == "__main__":
    analysis_flow()
