from flask import Flask, abort, render_template, request, Response, url_for
from dotenv import load_dotenv

from app.queries import (
    get_stats,
    get_latest_tenders,
    get_closing_soon,
    get_facet_list,
    search_tenders,
    get_tender_by_slug,
    get_documents,
    get_related_tenders,
    get_facet_page,
    get_combined_page,
)

load_dotenv()
app = Flask(__name__)


@app.template_filter("dt")
def format_dt(value):
    if not value:
        return "—"
    return value.strftime("%d %b %Y %H:%M") if hasattr(value, "strftime") else str(value)


@app.route("/")
def homepage():
    return render_template(
        "homepage.html",
        stats=get_stats(),
        latest_tenders=get_latest_tenders(),
        closing_soon=get_closing_soon(),
        top_provinces=get_facet_list("province"),
        top_categories=get_facet_list("category"),
    )


@app.route("/tenders")
def tenders():
    q = request.args.get("q", "").strip() or None
    province = request.args.get("province", "").strip() or None
    category = request.args.get("category", "").strip() or None
    items = search_tenders(q=q, province=province, category=category, active_only=False, limit=100)
    title = "All Tenders"
    if q:
        title = f"Search results for: {q}"
    return render_template("listing.html", title=title, items=items)


@app.route("/closing-soon")
def closing_soon_page():
    return render_template("listing.html", title="Closing Soon", items=get_closing_soon(limit=100))


@app.route("/province/<province_slug>")
def province_page(province_slug):
    facet, items = get_facet_page("province", province_slug, limit=100)
    if not facet:
        abort(404)
    title = f"{facet['facet_label']} Tenders"
    intro = f"Browse tender opportunities in {facet['facet_label']} by closing date, category, and organ of state."
    return render_template("facet_page.html", title=title, intro=intro, facet=facet, items=items, facet_type="province")


@app.route("/category/<category_slug>")
def category_page(category_slug):
    facet, items = get_facet_page("category", category_slug, limit=100)
    if not facet:
        abort(404)
    title = f"{facet['facet_label']} Tenders in South Africa"
    intro = f"Find {facet['facet_label']} tenders and procurement opportunities across South Africa."
    return render_template("facet_page.html", title=title, intro=intro, facet=facet, items=items, facet_type="category")


@app.route("/province/<province_slug>/category/<category_slug>")
def province_category_page(province_slug, category_slug):
    province, category, items = get_combined_page(province_slug, category_slug, limit=100)
    if not province or not category:
        abort(404)
    title = f"{category['facet_label']} Tenders in {province['facet_label']}"
    intro = f"Browse {category['facet_label']} tender opportunities in {province['facet_label']}."
    return render_template(
        "combined_page.html",
        title=title,
        intro=intro,
        province=province,
        category=category,
        items=items,
    )


@app.route("/tender/<slug>")
def tender_detail(slug):
    tender = get_tender_by_slug(slug)
    if not tender:
        abort(404)
    documents = get_documents(tender["id"])
    related = get_related_tenders(tender["id"], tender.get("category_slug"), tender.get("province_slug"))
    return render_template("tender_detail.html", tender=tender, documents=documents, related_tenders=related)
