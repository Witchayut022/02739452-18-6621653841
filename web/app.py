import os

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request

app = Flask(__name__)
app.jinja_env.filters["dict_update"] = lambda d, extra: {**d, **extra}


def query(sql, params=()):
    conn = psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "postgres"), dbname=os.getenv("DATA_DB_NAME", "pcparts"),
        user=os.getenv("POSTGRES_USER", "postgres"), password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    except psycopg2.errors.UndefinedTable:
        return []
    finally:
        conn.close()


@app.route("/")
def index():
    f = {k: request.args.get(k, "") for k in ("category", "shop", "brand", "q", "min_price", "max_price", "sort")}
    where, params = ["image_url IS NOT NULL AND image_url <> ''"], []  # show only products with a picture
    for col in ("category", "shop", "brand"):
        if f[col]:
            where.append(f"{col} = %s")
            params.append(f[col])
    if f["q"]:
        where.append("name ILIKE %s")
        params.append(f"%{f['q']}%")
    if f["min_price"].isdigit():
        where.append("price >= %s")
        params.append(int(f["min_price"]))
    if f["max_price"].isdigit():
        where.append("price <= %s")
        params.append(int(f["max_price"]))
    w = " AND ".join(where)

    kpi = (query(f"""SELECT count(*) AS n, round(avg(price)) AS avg_price, min(price) AS min_price,
                     max(price) AS max_price, count(DISTINCT shop) AS shops, count(DISTINCT brand) AS brands,
                     max(updated_at) AS updated FROM prices WHERE {w}""", params) or [{}])[0]
    order = {"price_desc": "price DESC", "name": "name"}.get(f["sort"], "price")
    per_page = 25
    pages = max(1, -(-int(kpi.get("n") or 0) // per_page))
    page = min(max(int(request.args.get("page", 1)) if request.args.get("page", "1").isdigit() else 1, 1), pages)
    rows = query(f"""SELECT category, brand, name, capacity_gb, interface, shop, price, url, image_url
                     FROM prices WHERE {w} ORDER BY {order}, name LIMIT {per_page} OFFSET {(page - 1) * per_page}""", params)
    base_args = {k: v for k, v in request.args.items() if k != "page" and v}
    by_shop = query(f"""SELECT shop, category, round(avg(price)) AS v
                        FROM prices WHERE {w} GROUP BY shop, category ORDER BY shop""", params)
    shops = sorted({r["shop"] for r in by_shop})
    datasets = [{"label": c, "data": [next((float(r["v"] or 0) for r in by_shop if r["shop"] == s and r["category"] == c), 0)
                                      for s in shops]} for c in ("SSD", "RAM")]
    opts = {
        "shops": [r["shop"] for r in query("SELECT DISTINCT shop FROM prices ORDER BY 1")],
        "brands": [r["brand"] for r in query("SELECT DISTINCT brand FROM prices ORDER BY 1")],
    }
    return render_template("index.html", kpi=kpi, rows=rows, f=f, opts=opts, page=page, pages=pages, base_args=base_args,
                           chart={"labels": shops, "datasets": datasets})


@app.route("/status")
def status():
    runs = query("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 40")
    dq = query("SELECT * FROM dq_reports ORDER BY id DESC LIMIT 5")
    return render_template("status.html", runs=runs, dq=dq)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
