import io
import sys
from flask import Flask, request, Response, stream_with_context
from pyspark.sql import SparkSession

app = Flask(__name__)

# One SparkSession shared across all queries — never torn down.
spark = SparkSession.builder.appName("lake-sql-server").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


@app.route("/health")
def health():
    return "ok", 200


@app.route("/query", methods=["POST"])
def query():
    body = request.get_json(silent=True) or {}
    sql  = (body.get("sql") or "").strip()
    if not sql:
        return "No SQL provided\n", 400

    def _run():
        try:
            df  = spark.sql(sql)
            # df.show() calls print() internally — redirect stdout to capture it.
            old = sys.stdout
            sys.stdout = buf = io.StringIO()
            try:
                df.show(500, truncate=False)
            finally:
                sys.stdout = old
            yield buf.getvalue()
        except Exception as exc:
            yield f"ERROR: {exc}\n"

    return Response(stream_with_context(_run()), mimetype="text/plain")


if __name__ == "__main__":
    # threaded=False — SparkSession is not thread-safe; queue queries sequentially.
    app.run(host="0.0.0.0", port=5002, threaded=False)
