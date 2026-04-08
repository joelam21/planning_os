from __future__ import annotations

import textwrap
import re
from datetime import datetime
from matplotlib import colors as mcolors

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd


def _month_label(month_start: str) -> str:
    """Format '2022-12-01' -> 'Dec 2022' for chart subtitles."""
    return datetime.strptime(month_start, "%Y-%m-%d").strftime("%b %Y")


def _annotate_vbar(ax, bar, annotation: str, max_h: float, inside_threshold: float, outside_offset: float) -> None:
    """Place a sales/YoY annotation on a vertical bar (inside or above)."""
    bar_h = bar.get_height()
    x_mid = bar.get_x() + bar.get_width() / 2
    if bar_h >= inside_threshold:
        y_pos = max(bar_h - max(bar_h * 0.06, 80), bar_h * 0.55)
        ax.text(x_mid, y_pos, annotation, ha="center", va="top", fontsize=9, color="white", fontweight="bold")
    else:
        ax.text(x_mid, bar_h + outside_offset, annotation, ha="center", va="bottom", fontsize=9, color="black", fontweight="bold")


def _literal_currency(text: str) -> str:
    """Escape dollar signs so matplotlib doesn't treat them as mathtext delimiters."""
    return text.replace("$", r"\$")


PRICE_POSITION_SEGMENT_ORDER = [
    "budget",
    "budget_standard",
    "high_volume",
    "standard",
    "premium",
    "premium_plus",
    "super_premium",
    "ultra_premium",
    "luxury",
    "icon_collectible",
    "bulk_or_bundle",
    "trial_size",
    "other",
    "unknown",
]

PRICE_POSITION_SEGMENT_DISPLAY_LABELS = {
    "budget_standard": "Budget/Standard",
    "premium_plus": "Premium+",
    "bulk_or_bundle": "Bulk/Bundle",
    "trial_size": "Trial Size",
}


def _display_price_segment_label(segment_name: str) -> str:
    """Return human-readable labels for display without changing raw segment values."""
    return PRICE_POSITION_SEGMENT_DISPLAY_LABELS.get(str(segment_name), str(segment_name))


def _display_vendor_name(vendor_name: str) -> str:
    """Return a clean vendor display name without numeric prefixes."""
    cleaned = re.sub(r"^\s*\d+\s*[-:]\s*", "", str(vendor_name)).strip()
    if cleaned.isupper():
        return cleaned.title()
    return cleaned


def _should_draw_segment_label(
    ax,
    segment_value_k: float,
    total_value_k: float,
    min_segment_pct: float = 9.0,
    min_pixel_height: float = 14.0,
) -> bool:
    """Gate segment labels by share and rendered height for readability."""
    if segment_value_k <= 0 or total_value_k <= 0:
        return False

    segment_pct = (segment_value_k / total_value_k) * 100
    if segment_pct < min_segment_pct:
        return False

    y_min, y_max = ax.get_ylim()
    axis_height_px = max(ax.bbox.height, 1.0)
    data_per_px = (y_max - y_min) / axis_height_px if y_max > y_min else 0
    min_height_k = data_per_px * min_pixel_height
    return segment_value_k >= min_height_k


def plot_family_growth(df_family: pd.DataFrame, month_start: str = "", trend_years: int = 3) -> None:
    """Horizontal bar chart of category family T12M sales with inside/outside annotations."""
    plot_df = df_family[df_family["category_family"] != "Grand Total"].copy()
    plot_df["sales_t12m_k"] = plot_df["sales_t12m"] / 1000.0
    plot_df = plot_df.sort_values("sales_t12m_k", ascending=True)

    fig, ax = plt.subplots(figsize=(12, 7))
    palette = plt.cm.tab10.colors
    colors = [palette[i % len(palette)] for i in range(len(plot_df))]
    bars = ax.barh(plot_df["category_family"], plot_df["sales_t12m_k"], color=colors)

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"Category Family Growth (T12M Sales){subtitle}")
    ax.set_xlabel("Sales T12M ($k)")
    ax.set_ylabel("Category Family")

    max_w = plot_df["sales_t12m_k"].max()
    inside_threshold = max_w * 0.15
    outside_offset = max(max_w * 0.015, 60)

    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        sales_label = f"${row['sales_t12m_k']:,.0f}k"
        yoy_val = row.get("t12m_yoy_pct")
        yoy_label = "n/a" if pd.isna(yoy_val) else f"{yoy_val:+.1f}%"
        cagr_start_year = row.get("cagr_start_year")
        cagr_end_year = row.get("cagr_end_year")
        cagr_start_sales = row.get("sales_cagr_start_year")
        cagr_end_sales = row.get("sales_cagr_end_year")
        cagr_label = "CAGR: n/a"
        if (
            pd.notna(cagr_start_year)
            and pd.notna(cagr_end_year)
            and pd.notna(cagr_start_sales)
            and pd.notna(cagr_end_sales)
        ):
            start_year = int(cagr_start_year)
            end_year = int(cagr_end_year)
            year_span = end_year - start_year
            start_sales = float(cagr_start_sales)
            end_sales = float(cagr_end_sales)
            if start_sales > 0 and end_sales > 0 and year_span > 0:
                cagr = ((end_sales / start_sales) ** (1 / year_span) - 1) * 100
                cagr_label = f"CAGR {start_year}-{end_year}: {cagr:+.1f}%"
        annotation = f"{sales_label} | {yoy_label}\n{cagr_label}"
        bar_w = bar.get_width()
        y_pos = bar.get_y() + bar.get_height() / 2

        if bar_w >= inside_threshold:
            x_pos = max(bar_w - max(bar_w * 0.04, 120), bar_w * 0.55)
            ax.text(x_pos, y_pos, annotation, va="center", ha="right", fontsize=9, color="white", fontweight="bold")
        else:
            ax.text(bar_w + outside_offset, y_pos, annotation, va="center", ha="left", fontsize=9, color="black", fontweight="bold")

    grand_total_rows = df_family[df_family["category_family"] == "Grand Total"]
    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_cagr_label = "CAGR: n/a"
        gt_start_year = gt.get("cagr_start_year")
        gt_end_year = gt.get("cagr_end_year")
        gt_start_sales = gt.get("sales_cagr_start_year")
        gt_end_sales = gt.get("sales_cagr_end_year")
        if (
            pd.notna(gt_start_year)
            and pd.notna(gt_end_year)
            and pd.notna(gt_start_sales)
            and pd.notna(gt_end_sales)
        ):
            start_year = int(gt_start_year)
            end_year = int(gt_end_year)
            year_span = end_year - start_year
            start_sales = float(gt_start_sales)
            end_sales = float(gt_end_sales)
            if start_sales > 0 and end_sales > 0 and year_span > 0:
                gt_cagr = ((end_sales / start_sales) ** (1 / year_span) - 1) * 100
                gt_cagr_label = f"CAGR {start_year}-{end_year}: {gt_cagr:+.1f}%"
        gt_text = f"Grand Total: ${gt_k:,.0f}k | {gt_yoy_label}\n{gt_cagr_label}"
        ax.text(0.5, 0.96, gt_text, transform=ax.transAxes, ha="center", va="center", fontsize=10, fontweight="bold", color="black",
                bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9))

    plt.tight_layout()
    plt.show()


def _build_other_row(rest: pd.DataFrame, key_col: str, key_val: str) -> dict:
    """Aggregate remaining rows into an 'Other' summary dict."""
    other_sales_t12m = rest["sales_t12m"].sum()
    other_sales_prior_t12m = rest["sales_prior_t12m"].sum()
    other_yoy_pct = None
    if other_sales_prior_t12m != 0:
        other_yoy_pct = ((other_sales_t12m - other_sales_prior_t12m) / other_sales_prior_t12m) * 100
    return {
        key_col: key_val,
        "sales_t12m": other_sales_t12m,
        "sales_prior_t12m": other_sales_prior_t12m,
        "t12m_yoy_pct": other_yoy_pct,
    }


def _blend_with_white(color: str | tuple, blend: float = 0.35) -> tuple[float, float, float, float]:
    """Lighten a color by blending it toward white."""
    r, g, b, a = mcolors.to_rgba(color)
    r = r + (1 - r) * blend
    g = g + (1 - g) * blend
    b = b + (1 - b) * blend
    return (r, g, b, a)


def plot_category_chart(df_category: pd.DataFrame, category_family: str, month_start: str = "", top_n: int = 5) -> None:
    """Vertical bar chart of top N categories + Other within a family, with sales/YoY annotations and grand total callout."""
    detail_rows = df_category[df_category["category_name"] != "Grand Total"].copy()
    grand_total_rows = df_category[df_category["category_name"] == "Grand Total"].copy()

    if len(detail_rows) == 0:
        print("No category rows returned.")
        return

    detail_rows = detail_rows.sort_values("sales_t12m", ascending=False).reset_index(drop=True)
    top = detail_rows.head(top_n).copy()
    rest = detail_rows.iloc[top_n:].copy()

    if len(rest) > 0:
        other = _build_other_row(rest, "category_name", "Other")
        plot_df = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
    else:
        plot_df = top.copy()

    plot_df["sales_t12m_k"] = plot_df["sales_t12m"] / 1000.0

    fig, ax = plt.subplots(figsize=(12, 6))
    palette = plt.cm.tab10.colors
    colors = [palette[i % len(palette)] for i in range(len(plot_df))]
    bars = ax.bar(range(len(plot_df)), plot_df["sales_t12m_k"], color=colors)

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"Top Categories (Top {top_n} + Other) - {category_family}{subtitle}")
    ax.set_xlabel("Category Name")
    ax.set_ylabel("Sales T12M ($k)")

    wrapped_labels = [textwrap.fill(str(name), width=14) for name in plot_df["category_name"]]
    ax.set_xticks(range(len(plot_df)))
    ax.set_xticklabels(wrapped_labels, ha="center", fontsize=9)

    max_h = plot_df["sales_t12m_k"].max()
    ax.set_ylim(0, max_h * 1.18)
    inside_threshold = max_h * 0.15
    outside_offset = max(max_h * 0.015, 60)

    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        sales_label = f"${row['sales_t12m_k']:,.0f}k"
        yoy_val = row.get("t12m_yoy_pct")
        yoy_label = "n/a" if pd.isna(yoy_val) else f"{yoy_val:+.1f}%"
        annotation = f"{sales_label}\n{yoy_label}"
        _annotate_vbar(ax, bar, annotation, max_h, inside_threshold, outside_offset)

    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_text = f"Family Total: ${gt_k:,.0f}k | {gt_yoy_label}"
        ax.text(0.5, 0.96, gt_text, transform=ax.transAxes, ha="center", va="center", fontsize=10, fontweight="bold", color="black",
                bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9))

    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.tight_layout()
    plt.show()


def plot_vendor_chart(df_detail: pd.DataFrame, category_name: str, month_start: str = "", top_n: int = 5) -> None:
    """Vertical bar chart of top N vendors + Other with wrapped name labels and grand total callout."""
    vendor_subtotals = df_detail[df_detail["row_type"] == "subtotal_by_vendor"].copy()
    grand_total_rows = df_detail[df_detail["row_type"] == "grand_total"].copy()

    if len(vendor_subtotals) == 0:
        print("No vendor subtotal rows returned for current filters.")
        return

    vendor_subtotals = vendor_subtotals.sort_values("sales_t12m", ascending=False)
    top = vendor_subtotals.head(top_n).copy()
    rest = vendor_subtotals.iloc[top_n:].copy()

    if len(rest) > 0:
        other = {**_build_other_row(rest, "vendor_number", "Other"), "vendor_name": "Other Vendors"}
        plot_df = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
    else:
        plot_df = top.copy()

    plot_df["vendor_number"] = plot_df["vendor_number"].astype(str)
    plot_df["vendor_name"] = plot_df["vendor_name"].fillna("Unknown Vendor").astype(str)
    plot_df["sales_t12m_k"] = plot_df["sales_t12m"] / 1000.0

    fig, ax = plt.subplots(figsize=(12, 6))
    palette = plt.cm.tab10.colors
    colors = [palette[i % len(palette)] for i in range(len(plot_df))]
    bars = ax.bar(plot_df["vendor_number"], plot_df["sales_t12m_k"], color=colors)

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"Top Vendors (Top {top_n} + Other) - {category_name}{subtitle}")
    ax.set_xlabel("Vendor Number")
    ax.set_ylabel("Sales T12M ($k)")
    plt.xticks(rotation=45, ha="right")

    max_h = plot_df["sales_t12m_k"].max()
    ax.set_ylim(0, max_h * 1.18)
    inside_threshold = max_h * 0.15
    outside_offset = max(max_h * 0.015, 60)

    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        sales_label = f"${row['sales_t12m_k']:,.0f}k"
        yoy_val = row.get("t12m_yoy_pct")
        yoy_label = "n/a" if pd.isna(yoy_val) else f"{yoy_val:+.1f}%"
        annotation = f"{sales_label}\n{yoy_label}"
        _annotate_vbar(ax, bar, annotation, max_h, inside_threshold, outside_offset)

        wrap_width = max(10, min(22, int(22 * (bar.get_height() / max_h))))
        wrapped_name = textwrap.fill(row["vendor_name"], width=wrap_width)
        y_base = max(bar.get_height() * 0.04, 45)
        ax.text(bar.get_x() + bar.get_width() / 2, y_base, wrapped_name, ha="center", va="bottom", fontsize=8, color="white", fontweight="bold", linespacing=1.0)

    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_text = f"Category Total: ${gt_k:,.0f}k | {gt_yoy_label}"
    else:
        gt_text = "Category Total: n/a"

    ax.text(0.5, 0.96, gt_text, transform=ax.transAxes, ha="center", va="center", fontsize=10, fontweight="bold", color="black",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9))

    plt.tight_layout()
    plt.show()


def plot_vendor_stacked_category_chart(
    df_detail: pd.DataFrame,
    category_family: str,
    month_start: str = "",
    top_n_vendors: int = 5,
) -> None:
    """Stacked vendor bar chart showing category mix within a category family."""
    detail_rows = df_detail[df_detail["row_type"] == "detail"].copy()
    grand_total_rows = df_detail[df_detail["row_type"] == "grand_total"].copy()

    if len(detail_rows) == 0:
        print("No vendor/category detail rows returned for current filters.")
        return

    vendor_totals = (
        detail_rows.groupby(["vendor_rank", "vendor_number", "vendor_name"], as_index=False)[["sales_t12m", "sales_prior_t12m"]]
        .sum()
        .sort_values(["vendor_rank", "sales_t12m"], ascending=[True, False])
        .reset_index(drop=True)
    )
    top_vendor_keys = vendor_totals.head(top_n_vendors)[["vendor_rank", "vendor_number", "vendor_name"]]

    top_detail_rows = detail_rows.merge(
        top_vendor_keys,
        on=["vendor_rank", "vendor_number", "vendor_name"],
        how="inner",
    ).copy()

    rest_vendor_totals = vendor_totals.iloc[top_n_vendors:].copy()
    if len(rest_vendor_totals) > 0:
        rest_detail_rows = detail_rows.merge(
            rest_vendor_totals[["vendor_rank", "vendor_number", "vendor_name"]],
            on=["vendor_rank", "vendor_number", "vendor_name"],
            how="inner",
        )
        other_detail_rows = (
            rest_detail_rows.groupby("category_name", as_index=False)[["sales_t12m", "sales_prior_t12m"]]
            .sum()
            .assign(vendor_rank=top_n_vendors + 1, vendor_number="Other", vendor_name="Other Vendors")
        )
        plot_rows = pd.concat([top_detail_rows, other_detail_rows], ignore_index=True, sort=False)
    else:
        plot_rows = top_detail_rows.copy()

    pivot_df = (
        plot_rows.pivot_table(
            index=["vendor_rank", "vendor_number", "vendor_name"],
            columns="category_name",
            values="sales_t12m",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
    )

    vendor_labels = [
        textwrap.fill(_display_vendor_name(vendor_name), width=14)
        for _, vendor_number, vendor_name in pivot_df.index
    ]

    fig, ax = plt.subplots(figsize=(13, 7))
    palette = plt.cm.tab20.colors
    bottom = pd.Series([0.0] * len(pivot_df), index=pivot_df.index)

    for i, category_name in enumerate(pivot_df.columns):
        values_k = pivot_df[category_name] / 1000.0
        ax.bar(
            range(len(pivot_df)),
            values_k,
            bottom=bottom / 1000.0,
            color=palette[i % len(palette)],
            label=category_name,
        )
        bottom += pivot_df[category_name]

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    title_suffix = f"Top {top_n_vendors} + Other"
    ax.set_title(f"Vendor Category Mix ({title_suffix}) - {category_family}{subtitle}")
    ax.set_xlabel("Vendor")
    ax.set_ylabel("Sales T12M ($k)")
    ax.set_xticks(range(len(pivot_df)))
    ax.set_xticklabels(vendor_labels, rotation=0, ha="center", fontsize=8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(title="Category", loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=min(4, max(1, len(pivot_df.columns))))

    totals_k = bottom / 1000.0
    max_h = totals_k.max()
    ax.set_ylim(0, max_h * 1.2)
    for idx, total_k in enumerate(totals_k):
        ax.text(idx, total_k + max(max_h * 0.015, 40), f"${total_k:,.0f}k", ha="center", va="bottom", fontsize=9, fontweight="bold")

    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_text = f"Family Total: ${gt_k:,.0f}k | {gt_yoy_label}"
        ax.text(
            0.5,
            0.96,
            gt_text,
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="black",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9),
        )

    plt.tight_layout()
    plt.show()


def plot_vendor_stacked_price_segment_chart(
    df_detail: pd.DataFrame,
    category_family: str,
    month_start: str = "",
    top_n_vendors: int = 5,
) -> None:
    """Stacked vendor bar chart showing price-position mix within a category family."""
    detail_rows = df_detail[df_detail["row_type"] == "detail"].copy()
    grand_total_rows = df_detail[df_detail["row_type"] == "grand_total"].copy()

    if len(detail_rows) == 0:
        print("No vendor/price-segment detail rows returned for current filters.")
        return

    vendor_totals = (
        detail_rows.groupby(["vendor_rank", "vendor_number", "vendor_name"], as_index=False)[["sales_t12m", "sales_prior_t12m", "units_t12m"]]
        .sum()
        .sort_values(["vendor_rank", "sales_t12m"], ascending=[True, False])
        .reset_index(drop=True)
    )
    vendor_totals["avg_selling_price"] = vendor_totals["sales_t12m"] / vendor_totals["units_t12m"]
    vendor_totals.loc[vendor_totals["units_t12m"] == 0, "avg_selling_price"] = pd.NA

    top_vendor_keys = vendor_totals.head(top_n_vendors)[["vendor_rank", "vendor_number", "vendor_name"]]

    top_detail_rows = detail_rows.merge(
        top_vendor_keys,
        on=["vendor_rank", "vendor_number", "vendor_name"],
        how="inner",
    ).copy()

    rest_vendor_totals = vendor_totals.iloc[top_n_vendors:].copy()
    if len(rest_vendor_totals) > 0:
        rest_detail_rows = detail_rows.merge(
            rest_vendor_totals[["vendor_rank", "vendor_number", "vendor_name"]],
            on=["vendor_rank", "vendor_number", "vendor_name"],
            how="inner",
        )
        other_detail_rows = (
            rest_detail_rows.groupby("price_position_segment", as_index=False)[["sales_t12m", "sales_prior_t12m", "units_t12m"]]
            .sum()
            .assign(vendor_rank=top_n_vendors + 1, vendor_number="Other", vendor_name="Other Vendors")
        )
        plot_rows = pd.concat([top_detail_rows, other_detail_rows], ignore_index=True, sort=False)
    else:
        plot_rows = top_detail_rows.copy()

    pivot_df = (
        plot_rows.pivot_table(
            index=["vendor_rank", "vendor_number", "vendor_name"],
            columns="price_position_segment",
            values="sales_t12m",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
    )

    ordered_columns = [segment for segment in PRICE_POSITION_SEGMENT_ORDER if segment in pivot_df.columns]
    unordered_columns = [segment for segment in pivot_df.columns if segment not in ordered_columns]
    pivot_df = pivot_df[ordered_columns + unordered_columns]

    plot_vendor_totals = (
        plot_rows.groupby(["vendor_rank", "vendor_number", "vendor_name"], as_index=False)[["sales_t12m", "units_t12m"]]
        .sum()
        .sort_values(["vendor_rank", "sales_t12m"], ascending=[True, False])
    )
    plot_vendor_totals["avg_selling_price"] = plot_vendor_totals["sales_t12m"] / plot_vendor_totals["units_t12m"]
    plot_vendor_totals.loc[plot_vendor_totals["units_t12m"] == 0, "avg_selling_price"] = pd.NA
    asp_lookup = plot_vendor_totals.set_index(["vendor_rank", "vendor_number", "vendor_name"])["avg_selling_price"]

    vendor_labels = [
        textwrap.fill(_display_vendor_name(vendor_name), width=14)
        for _, vendor_number, vendor_name in pivot_df.index
    ]

    fig, ax = plt.subplots(figsize=(13, 7))
    palette = plt.cm.tab20.colors
    bottom = pd.Series([0.0] * len(pivot_df), index=pivot_df.index)

    for i, segment_name in enumerate(pivot_df.columns):
        values_k = pivot_df[segment_name] / 1000.0
        ax.bar(
            range(len(pivot_df)),
            values_k,
            bottom=bottom / 1000.0,
            color=palette[i % len(palette)],
            label=_display_price_segment_label(segment_name),
        )
        bottom += pivot_df[segment_name]

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    title_suffix = f"Top {top_n_vendors} + Other"
    ax.set_title(f"Vendor Price Segment Mix ({title_suffix}) - {category_family}{subtitle}")
    ax.set_xlabel("Vendor")
    ax.set_ylabel("Sales T12M ($k)")
    ax.set_xticks(range(len(pivot_df)))
    ax.set_xticklabels(vendor_labels, rotation=0, ha="center", fontsize=8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    totals_k = bottom / 1000.0
    max_h = totals_k.max()
    ax.set_ylim(0, max_h * 1.24)
    for idx, (_, row) in enumerate(pivot_df.iterrows()):
        running_k = 0.0
        total_k = totals_k.iloc[idx]
        for segment_name in pivot_df.columns:
            segment_k = row[segment_name] / 1000.0
            if not _should_draw_segment_label(ax, segment_k, total_k):
                running_k += segment_k
                continue
            ax.text(
                idx,
                running_k + (segment_k / 2),
                _display_price_segment_label(segment_name),
                ha="center",
                va="center",
                fontsize=8,
                color="white",
                fontweight="bold",
            )
            running_k += segment_k

    for idx, (index_key, total_k) in enumerate(zip(pivot_df.index, totals_k)):
        avg_price = asp_lookup.get(index_key)
        avg_price_label = "n/a" if pd.isna(avg_price) else f"${avg_price:,.2f}"
        ax.text(
            idx,
            total_k + max(max_h * 0.015, 40),
            _literal_currency(f"${total_k:,.0f}k\nASP {avg_price_label}"),
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_asp = gt.get("avg_selling_price")
        gt_asp_label = "n/a" if pd.isna(gt_asp) else f"${gt_asp:,.2f}"
        gt_text = _literal_currency(f"Family Total: ${gt_k:,.0f}k | {gt_yoy_label} | ASP {gt_asp_label}")
        ax.text(
            0.5,
            0.96,
            gt_text,
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="black",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9),
        )

    plt.tight_layout()
    plt.show()


STORE_CHANNEL_ORDER = [
    "Grocery",
    "Warehouse/Club",
    "Mass Merchandise",
    "Drug",
    "Convenience/Gas",
    "Specialty/Tobacco",
    "Liquor Store",
    "On-Premise",
    "Winery/Distillery",
    "Unknown",
]

STORE_CHANNEL_COLORS = {
    "Grocery":           "#4CAF50",
    "Warehouse/Club":    "#2196F3",
    "Mass Merchandise":  "#9C27B0",
    "Drug":              "#F44336",
    "Convenience/Gas":   "#FF9800",
    "Specialty/Tobacco": "#795548",
    "Liquor Store":      "#3F51B5",
    "On-Premise":        "#009688",
    "Winery/Distillery": "#E91E63",
    "Unknown":           "#9E9E9E",
}


def plot_vendor_stacked_store_channel_chart(
    df_detail: pd.DataFrame,
    category_family: str,
    month_start: str = "",
    top_n_vendors: int = 5,
) -> None:
    """Stacked vendor bar chart showing store channel mix within a category family."""
    detail_rows = df_detail[df_detail["row_type"] == "detail"].copy()
    grand_total_rows = df_detail[df_detail["row_type"] == "grand_total"].copy()

    if len(detail_rows) == 0:
        print("No vendor/store-channel detail rows returned for current filters.")
        return

    vendor_totals = (
        detail_rows.groupby(["vendor_rank", "vendor_number", "vendor_name"], as_index=False)[["sales_t12m", "sales_prior_t12m", "units_t12m"]]
        .sum()
        .sort_values(["vendor_rank", "sales_t12m"], ascending=[True, False])
        .reset_index(drop=True)
    )
    vendor_totals["avg_selling_price"] = vendor_totals["sales_t12m"] / vendor_totals["units_t12m"]
    vendor_totals.loc[vendor_totals["units_t12m"] == 0, "avg_selling_price"] = pd.NA

    top_vendor_keys = vendor_totals.head(top_n_vendors)[["vendor_rank", "vendor_number", "vendor_name"]]

    top_detail_rows = detail_rows.merge(
        top_vendor_keys,
        on=["vendor_rank", "vendor_number", "vendor_name"],
        how="inner",
    ).copy()

    rest_vendor_totals = vendor_totals.iloc[top_n_vendors:].copy()
    if len(rest_vendor_totals) > 0:
        rest_detail_rows = detail_rows.merge(
            rest_vendor_totals[["vendor_rank", "vendor_number", "vendor_name"]],
            on=["vendor_rank", "vendor_number", "vendor_name"],
            how="inner",
        )
        other_detail_rows = (
            rest_detail_rows.groupby("store_channel", as_index=False)[["sales_t12m", "sales_prior_t12m", "units_t12m"]]
            .sum()
            .assign(vendor_rank=top_n_vendors + 1, vendor_number="Other", vendor_name="Other Vendors")
        )
        plot_rows = pd.concat([top_detail_rows, other_detail_rows], ignore_index=True, sort=False)
    else:
        plot_rows = top_detail_rows.copy()

    pivot_df = (
        plot_rows.pivot_table(
            index=["vendor_rank", "vendor_number", "vendor_name"],
            columns="store_channel",
            values="sales_t12m",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
    )

    ordered_columns = [ch for ch in STORE_CHANNEL_ORDER if ch in pivot_df.columns]
    unordered_columns = [ch for ch in pivot_df.columns if ch not in ordered_columns]
    pivot_df = pivot_df[ordered_columns + unordered_columns]

    plot_vendor_totals = (
        plot_rows.groupby(["vendor_rank", "vendor_number", "vendor_name"], as_index=False)[["sales_t12m", "units_t12m"]]
        .sum()
        .sort_values(["vendor_rank", "sales_t12m"], ascending=[True, False])
    )
    plot_vendor_totals["avg_selling_price"] = plot_vendor_totals["sales_t12m"] / plot_vendor_totals["units_t12m"]
    plot_vendor_totals.loc[plot_vendor_totals["units_t12m"] == 0, "avg_selling_price"] = pd.NA
    asp_lookup = plot_vendor_totals.set_index(["vendor_rank", "vendor_number", "vendor_name"])["avg_selling_price"]
    vendor_sales_lookup = plot_vendor_totals.set_index(["vendor_rank", "vendor_number", "vendor_name"])["sales_t12m"]

    # ASP per vendor × channel for segment annotations
    channel_asp_df = (
        plot_rows.groupby(["vendor_rank", "vendor_number", "vendor_name", "store_channel"], as_index=False)[["sales_t12m", "units_t12m"]]
        .sum()
    )
    channel_asp_df["channel_asp"] = channel_asp_df["sales_t12m"] / channel_asp_df["units_t12m"].replace(0, pd.NA)
    channel_asp_lookup = channel_asp_df.set_index(["vendor_rank", "vendor_number", "vendor_name", "store_channel"])["channel_asp"]

    vendor_labels = [
        textwrap.fill(_display_vendor_name(vendor_name), width=14)
        for _, vendor_number, vendor_name in pivot_df.index
    ]

    fig, ax = plt.subplots(figsize=(13, 7))
    bottom = pd.Series([0.0] * len(pivot_df), index=pivot_df.index)

    for channel_name in pivot_df.columns:
        values_k = pivot_df[channel_name] / 1000.0
        color = STORE_CHANNEL_COLORS.get(channel_name, "#BDBDBD")
        ax.bar(
            range(len(pivot_df)),
            values_k,
            bottom=bottom / 1000.0,
            color=color,
            label=channel_name,
        )
        bottom += pivot_df[channel_name]

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    title_suffix = f"Top {top_n_vendors} + Other"
    ax.set_title(f"Vendor Store Channel Mix ({title_suffix}) - {category_family}{subtitle}")
    ax.set_xlabel("Vendor")
    ax.set_ylabel("Sales T12M ($k)")
    ax.set_xticks(range(len(pivot_df)))
    ax.set_xticklabels(vendor_labels, rotation=0, ha="center", fontsize=8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    totals_k = bottom / 1000.0
    max_h = totals_k.max()
    ax.set_ylim(0, max_h * 1.24)

    for idx, (index_key, row) in enumerate(pivot_df.iterrows()):
        running_k = 0.0
        total_k = totals_k.iloc[idx]
        vendor_total_sales = vendor_sales_lookup.get(index_key, 0)
        for channel_name in pivot_df.columns:
            segment_k = row[channel_name] / 1000.0
            if not _should_draw_segment_label(ax, segment_k, total_k):
                running_k += segment_k
                continue
            pct = (row[channel_name] / vendor_total_sales * 100) if vendor_total_sales > 0 else 0
            ch_asp = channel_asp_lookup.get((*index_key, channel_name))
            asp_str = "n/a" if pd.isna(ch_asp) else _literal_currency(f"${ch_asp:,.2f}")
            label = f"{channel_name}\n{pct:.0f}% | {asp_str}"
            ax.text(
                idx,
                running_k + (segment_k / 2),
                label,
                ha="center",
                va="center",
                fontsize=7.5,
                color="white",
                fontweight="bold",
            )
            running_k += segment_k

    for idx, (index_key, total_k) in enumerate(zip(pivot_df.index, totals_k)):
        avg_price = asp_lookup.get(index_key)
        avg_price_label = "n/a" if pd.isna(avg_price) else f"${avg_price:,.2f}"
        ax.text(
            idx,
            total_k + max(max_h * 0.015, 40),
            _literal_currency(f"${total_k:,.0f}k\nASP {avg_price_label}"),
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_asp = gt.get("avg_selling_price")
        gt_asp_label = "n/a" if pd.isna(gt_asp) else f"${gt_asp:,.2f}"
        gt_text = _literal_currency(f"Family Total: ${gt_k:,.0f}k | {gt_yoy_label} | ASP {gt_asp_label}")
        ax.text(
            0.5, 0.96,
            gt_text,
            transform=ax.transAxes,
            ha="center", va="center",
            fontsize=10, fontweight="bold", color="black",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9),
        )

    ax.legend(
        loc="upper right",
        fontsize=8,
        framealpha=0.85,
        title="Store Channel",
        title_fontsize=8,
    )

    plt.tight_layout()
    plt.show()


def plot_vendor_store_channel_compare(
    df_detail: pd.DataFrame,
    category_family: str,
    month_start: str = "",
    title_override: str | None = None,
) -> None:
    """Side-by-side earliest vs latest FY stacked vendor store channel mix for top 3 current-year vendors + Other."""
    if len(df_detail) == 0:
        print("No vendor/store-channel comparison rows returned for current filters.")
        return

    period_frames = []
    for period_order, period_df in df_detail.groupby("period_order"):
        period_frames.append((period_order, period_df.copy()))
    period_frames = sorted(period_frames, key=lambda x: x[0], reverse=True)

    if len(period_frames) > 2:
        period_frames = [period_frames[0], period_frames[-1]]

    fig, axes = plt.subplots(1, len(period_frames), figsize=(14, 6), sharey=True)
    if len(period_frames) == 1:
        axes = [axes]

    all_totals = []
    subplot_payloads = []
    for _, period_df in period_frames:
        pivot_df = (
            period_df.pivot_table(
                index=["vendor_rank", "vendor_number", "vendor_name"],
                columns="store_channel",
                values="sales_t12m",
                aggfunc="sum",
                fill_value=0,
            )
            .sort_index()
        )
        ordered_columns = [ch for ch in STORE_CHANNEL_ORDER if ch in pivot_df.columns]
        unordered_columns = [ch for ch in pivot_df.columns if ch not in ordered_columns]
        pivot_df = pivot_df[ordered_columns + unordered_columns]

        vendor_totals = (
            period_df.groupby(["vendor_rank", "vendor_number", "vendor_name"], as_index=False)[["sales_t12m", "units_t12m"]]
            .sum()
            .sort_values(["vendor_rank", "sales_t12m"], ascending=[True, False])
        )
        vendor_totals["avg_selling_price"] = vendor_totals["sales_t12m"] / vendor_totals["units_t12m"]
        vendor_totals.loc[vendor_totals["units_t12m"] == 0, "avg_selling_price"] = pd.NA
        asp_lookup = vendor_totals.set_index(["vendor_rank", "vendor_number", "vendor_name"])["avg_selling_price"]

        totals = pivot_df.sum(axis=1)
        all_totals.extend((totals / 1000.0).tolist())
        subplot_payloads.append((period_df["period_year"].iloc[0], pivot_df, asp_lookup, totals))

    max_h = max(all_totals) if all_totals else 0

    for ax, (period_year, pivot_df, asp_lookup, totals) in zip(axes, subplot_payloads):
        vendor_labels = [
            textwrap.fill(_display_vendor_name(vendor_name), width=14)
            for _, vendor_number, vendor_name in pivot_df.index
        ]
        bottom = pd.Series([0.0] * len(pivot_df), index=pivot_df.index)

        for channel_name in pivot_df.columns:
            values = pivot_df[channel_name]
            values_k = values / 1000.0
            color = STORE_CHANNEL_COLORS.get(channel_name, "#BDBDBD")
            bars = ax.bar(
                range(len(pivot_df)),
                values_k,
                bottom=bottom / 1000.0,
                color=color,
                label=channel_name,
            )
            for idx, (bar, value, total_value) in enumerate(zip(bars, values, totals)):
                value_k = value / 1000.0
                total_k = total_value / 1000.0
                if not _should_draw_segment_label(ax, value_k, total_k):
                    continue
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    (bottom.iloc[idx] + value / 2) / 1000.0,
                    channel_name,
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="white",
                    fontweight="bold",
                )
            bottom += values

        totals_k = totals / 1000.0
        for idx, (index_key, total_k) in enumerate(zip(pivot_df.index, totals_k)):
            avg_price = asp_lookup.get(index_key)
            avg_price_label = "n/a" if pd.isna(avg_price) else f"${avg_price:,.2f}"
            ax.text(
                idx,
                total_k + max(max_h * 0.015, 40),
                _literal_currency(f"${total_k:,.0f}k\nASP {avg_price_label}"),
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
            )

        ax.set_title(f"FY {period_year}")
        ax.set_xlabel("Vendor")
        ax.set_xticks(range(len(pivot_df)))
        ax.set_xticklabels(vendor_labels, rotation=0, ha="center", fontsize=8)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
        ax.set_ylim(0, max_h * 1.24)

    axes[0].set_ylabel("Sales T12M ($k)")

    handles = [
        mpatches.Patch(color=STORE_CHANNEL_COLORS.get(ch, "#BDBDBD"), label=ch)
        for ch in STORE_CHANNEL_ORDER
        if any(ch in payload[1].columns for payload in subplot_payloads)
    ]
    fig.legend(
        handles=handles,
        title="Store Channel",
        loc="lower center",
        ncol=min(5, len(handles)),
        fontsize=8,
        title_fontsize=8,
        bbox_to_anchor=(0.5, -0.02),
    )

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    chart_title = title_override or f"Vendor Store Channel Mix Comparison (Top 3 + Other) - {category_family}{subtitle}"
    fig.suptitle(chart_title, fontsize=14, fontweight="bold", y=0.98)

    plt.tight_layout(rect=[0, 0.08, 1, 0.94])
    plt.show()


def plot_vendor_share_donuts_3y(
    df_detail: pd.DataFrame,
    category_family: str,
    month_start: str = "",
    top_n_vendors: int = 5,
    title_override: str | None = None,
) -> None:
    """Earliest-vs-latest donut charts for top vendors + Other across a trailing annual range."""
    if len(df_detail) == 0:
        print("No vendor share rows returned for current filters.")
        return

    period_frames = []
    for period_order, period_df in df_detail.groupby("period_order"):
        period_df = period_df.sort_values("sales_t12m", ascending=False).reset_index(drop=True)
        top = period_df.head(top_n_vendors).copy()
        rest = period_df.iloc[top_n_vendors:].copy()

        if len(rest) > 0:
            other = {
                "vendor_number": "Other",
                "sales_t12m": rest["sales_t12m"].sum(),
                "vendor_name": "Other Vendors",
                "period_order": period_df.iloc[0]["period_order"],
                "period_year": period_df.iloc[0]["period_year"],
                "period_label": period_df.iloc[0]["period_label"],
            }
            plot_df = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
        else:
            plot_df = top.copy()

        period_frames.append((period_order, plot_df))

    period_frames = sorted(period_frames, key=lambda x: x[0], reverse=True)
    if len(period_frames) > 1:
        period_frames = [period_frames[0], period_frames[-1]]

    fig, axes = plt.subplots(1, len(period_frames), figsize=(11, 5))
    if len(period_frames) == 1:
        axes = [axes]

    palette = list(plt.cm.tab10.colors) + list(plt.cm.Set3.colors)
    vendor_order = (
        df_detail.groupby(["vendor_number", "vendor_name"], as_index=False)["sales_t12m"]
        .sum()
        .sort_values(["sales_t12m", "vendor_number"], ascending=[False, True])
    )
    color_map: dict[str, tuple[float, float, float, float] | tuple[float, float, float]] = {}
    palette_idx = 0
    for _, row in vendor_order.iterrows():
        vendor_number = str(row["vendor_number"])
        if vendor_number == "Other":
            continue
        color_map[vendor_number] = palette[palette_idx % len(palette)]
        palette_idx += 1
    color_map["Other"] = "#8c564b"

    def _autopct(pct: float) -> str:
        return f"{pct:.1f}%" if pct >= 5 else ""

    selected_period_orders = [period_order for period_order, _ in period_frames]

    for ax, (_, period_df) in zip(axes, period_frames):
        values = period_df["sales_t12m"].tolist()
        colors = []
        for _, row in period_df.iterrows():
            base_color = color_map.get(str(row["vendor_number"]), "#7f7f7f")
            if "DIAGEO" in str(row["vendor_name"]).upper():
                colors.append(base_color)
            else:
                colors.append(_blend_with_white(base_color, blend=0.50))
        explode = [
            0.06 if "DIAGEO" in str(row["vendor_name"]).upper() else 0.0
            for _, row in period_df.iterrows()
        ]

        wedges, _, autotexts = ax.pie(
            values,
            labels=None,
            colors=colors,
            explode=explode,
            startangle=90,
            counterclock=False,
            autopct=_autopct,
            pctdistance=0.78,
            labeldistance=1.08,
            wedgeprops=dict(width=0.42, edgecolor="white"),
            textprops=dict(fontsize=8),
        )
        for autotext in autotexts:
            autotext.set_fontsize(8)
            autotext.set_fontweight("bold")

        period_year = int(period_df.iloc[0]["period_year"])
        total_k = period_df["sales_t12m"].sum() / 1000.0
        ax.set_title(f"FY {period_year}\n${total_k:,.0f}k", fontsize=10, fontweight="bold")
        ax.set_aspect("equal")

        rank_df = period_df[period_df["vendor_number"].astype(str) != "Other"].copy()
        rank_df = rank_df.sort_values("sales_t12m", ascending=False).reset_index(drop=True)
        diageo_rows = rank_df[rank_df["vendor_name"].astype(str).str.upper().str.contains("DIAGEO", na=False)]
        if len(diageo_rows) > 0:
            diageo_rank = int(diageo_rows.index[0]) + 1
            center_label = f"Diageo\n#{diageo_rank}"
        else:
            center_label = "Diageo\nn/a"
        ax.text(
            0,
            0,
            center_label,
            ha="center",
            va="center",
            fontsize=11,
            fontweight="bold",
            color="#333333",
        )

        legend_labels = [
            "Other Vendors" if row["vendor_number"] == "Other" else _display_vendor_name(row["vendor_name"])
            for _, row in period_df.iterrows()
        ]
        ax.legend(
            wedges,
            legend_labels,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.25),
            fontsize=8,
            frameon=False,
        )

    if len(period_frames) == 2:
        cagr_source = df_detail[df_detail["period_order"].isin(selected_period_orders)].copy()
        brand_specs = [
            ("Diageo", "DIAGEO"),
            ("Proximo", "PROXIMO"),
            ("Bacardi", "BACARDI"),
        ]
        cagr_lines: list[str] = []
        for brand_label, brand_match in brand_specs:
            brand_df = cagr_source[cagr_source["vendor_name"].astype(str).str.upper().str.contains(brand_match, na=False)].copy()
            if len(brand_df) == 0:
                cagr_lines.append(f"{brand_label}: n/a")
                continue

            brand_annual = (
                brand_df.groupby("period_year", as_index=False)["sales_t12m"]
                .sum()
                .sort_values("period_year")
            )
            if len(brand_annual) < 2:
                cagr_lines.append(f"{brand_label}: n/a")
                continue

            start_year = int(brand_annual.iloc[0]["period_year"])
            end_year = int(brand_annual.iloc[-1]["period_year"])
            start_val = float(brand_annual.iloc[0]["sales_t12m"])
            end_val = float(brand_annual.iloc[-1]["sales_t12m"])
            year_span = end_year - start_year
            if start_val > 0 and end_val > 0 and year_span > 0:
                cagr = ((end_val / start_val) ** (1 / year_span) - 1) * 100
                cagr_lines.append(f"{brand_label}: {cagr:+.1f}%")
            else:
                cagr_lines.append(f"{brand_label}: n/a")

        diageo_rank_labels: list[str] = []
        for _, period_df in period_frames:
            rank_df = period_df[period_df["vendor_number"].astype(str) != "Other"].copy()
            rank_df = rank_df.sort_values("sales_t12m", ascending=False).reset_index(drop=True)
            diageo_rows = rank_df[rank_df["vendor_name"].astype(str).str.upper().str.contains("DIAGEO", na=False)]
            if len(diageo_rows) > 0:
                diageo_rank_labels.append(f"#{int(diageo_rows.index[0]) + 1}")
            else:
                diageo_rank_labels.append("n/a")
        market_share_note = f"Diageo market share: {diageo_rank_labels[0]} \u2192 {diageo_rank_labels[-1]}"

        fig.text(
            0.5,
            0.72,
            market_share_note,
            ha="center",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="#333333",
        )

        fig.text(
            0.5,
            0.48,
            "CAGR\n" + "\n".join(cagr_lines),
            ha="center",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="#333333",
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="0.75", alpha=0.95),
        )

    chart_title = title_override or f"Vendor Share Donuts (Top {top_n_vendors} + Other) - {category_family}"
    fig.suptitle(
        chart_title,
        fontsize=12,
        fontweight="bold",
        y=1.02,
    )
    if len(period_frames) == 2:
        fig.subplots_adjust(left=0.12, right=0.88, wspace=0.18, bottom=0.18, top=0.82)
    else:
        fig.subplots_adjust(left=0.08, right=0.92, wspace=0.30, bottom=0.18, top=0.82)
    plt.show()


def plot_vendor_price_segment_compare(
    df_detail: pd.DataFrame,
    category_family: str,
    month_start: str = "",
    title_override: str | None = None,
) -> None:
    """Side-by-side earliest vs latest FY stacked vendor segment mix for top 3 current-year vendors + Other."""
    if len(df_detail) == 0:
        print("No vendor/price-segment comparison rows returned for current filters.")
        return

    period_frames = []
    for period_order, period_df in df_detail.groupby("period_order"):
        period_frames.append((period_order, period_df.copy()))
    period_frames = sorted(period_frames, key=lambda x: x[0], reverse=True)

    if len(period_frames) > 2:
        period_frames = [period_frames[0], period_frames[-1]]

    fig, axes = plt.subplots(1, len(period_frames), figsize=(14, 6), sharey=True)
    if len(period_frames) == 1:
        axes = [axes]

    palette = plt.cm.tab20.colors
    color_map = {
        segment: palette[i % len(palette)]
        for i, segment in enumerate(PRICE_POSITION_SEGMENT_ORDER)
    }

    all_totals = []
    subplot_payloads = []
    for _, period_df in period_frames:
        pivot_df = (
            period_df.pivot_table(
                index=["vendor_rank", "vendor_number", "vendor_name"],
                columns="price_position_segment",
                values="sales_t12m",
                aggfunc="sum",
                fill_value=0,
            )
            .sort_index()
        )
        ordered_columns = [segment for segment in PRICE_POSITION_SEGMENT_ORDER if segment in pivot_df.columns]
        unordered_columns = [segment for segment in pivot_df.columns if segment not in ordered_columns]
        pivot_df = pivot_df[ordered_columns + unordered_columns]

        vendor_totals = (
            period_df.groupby(["vendor_rank", "vendor_number", "vendor_name"], as_index=False)[["sales_t12m", "units_t12m"]]
            .sum()
            .sort_values(["vendor_rank", "sales_t12m"], ascending=[True, False])
        )
        vendor_totals["avg_selling_price"] = vendor_totals["sales_t12m"] / vendor_totals["units_t12m"]
        vendor_totals.loc[vendor_totals["units_t12m"] == 0, "avg_selling_price"] = pd.NA
        asp_lookup = vendor_totals.set_index(["vendor_rank", "vendor_number", "vendor_name"])["avg_selling_price"]

        totals = pivot_df.sum(axis=1)
        all_totals.extend((totals / 1000.0).tolist())
        subplot_payloads.append((period_df["period_year"].iloc[0], pivot_df, asp_lookup, totals))

    max_h = max(all_totals) if all_totals else 0

    for ax, (period_year, pivot_df, asp_lookup, totals) in zip(axes, subplot_payloads):
        vendor_labels = [
            textwrap.fill(_display_vendor_name(vendor_name), width=14)
            for _, vendor_number, vendor_name in pivot_df.index
        ]
        bottom = pd.Series([0.0] * len(pivot_df), index=pivot_df.index)

        for segment_name in pivot_df.columns:
            values = pivot_df[segment_name]
            values_k = values / 1000.0
            bars = ax.bar(
                range(len(pivot_df)),
                values_k,
                bottom=bottom / 1000.0,
                color=color_map.get(segment_name, "#7f7f7f"),
                label=_display_price_segment_label(segment_name),
            )
            for idx, (bar, value, total_value) in enumerate(zip(bars, values, totals)):
                value_k = value / 1000.0
                total_k = total_value / 1000.0
                if not _should_draw_segment_label(ax, value_k, total_k):
                    continue
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    (bottom.iloc[idx] + value / 2) / 1000.0,
                    _display_price_segment_label(segment_name),
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="white",
                    fontweight="bold",
                )
            bottom += values

        totals_k = totals / 1000.0
        for idx, (index_key, total_k) in enumerate(zip(pivot_df.index, totals_k)):
            avg_price = asp_lookup.get(index_key)
            avg_price_label = "n/a" if pd.isna(avg_price) else f"${avg_price:,.2f}"
            ax.text(
                idx,
                total_k + max(max_h * 0.015, 40),
                _literal_currency(f"${total_k:,.0f}k\nASP {avg_price_label}"),
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
            )

        ax.set_title(f"FY {period_year}")
        ax.set_xlabel("Vendor")
        ax.set_xticks(range(len(pivot_df)))
        ax.set_xticklabels(vendor_labels, rotation=0, ha="center", fontsize=8)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
        ax.set_ylim(0, max_h * 1.24)

    axes[0].set_ylabel("Sales T12M ($k)")

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    chart_title = title_override or f"Vendor Price Segment Mix Comparison (Top 3 + Other) - {category_family}{subtitle}"
    fig.suptitle(chart_title, fontsize=14, fontweight="bold", y=0.98)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.show()


def plot_volume_chart(df_detail: pd.DataFrame, category_name: str, month_start: str = "", top_n: int = 5) -> None:
    """Vertical bar chart of top N bottle volumes + Other with sales/YoY annotations."""
    volume_subtotals = df_detail[df_detail["row_type"] == "subtotal_by_bottle_volume"].copy()

    if len(volume_subtotals) == 0:
        print("No bottle volume subtotal rows returned for current filters.")
        return

    volume_subtotals = volume_subtotals.sort_values("sales_t12m", ascending=False)
    top = volume_subtotals.head(top_n).copy()
    rest = volume_subtotals.iloc[top_n:].copy()

    if len(rest) > 0:
        other = _build_other_row(rest, "bottle_volume_ml", "Other")
        plot_df = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
    else:
        plot_df = top.copy()

    plot_df["bottle_volume_ml"] = plot_df["bottle_volume_ml"].astype(str)
    plot_df["sales_t12m_k"] = plot_df["sales_t12m"] / 1000.0

    fig, ax = plt.subplots(figsize=(12, 6))
    palette = plt.cm.tab10.colors
    colors = [palette[i % len(palette)] for i in range(len(plot_df))]
    bars = ax.bar(plot_df["bottle_volume_ml"], plot_df["sales_t12m_k"], color=colors)

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"Top Bottle Volumes (Top {top_n} + Other) - {category_name}{subtitle}")
    ax.set_xlabel("Bottle Volume (ml)")
    ax.set_ylabel("Sales T12M ($k)")
    plt.xticks(rotation=45, ha="right")

    max_h = plot_df["sales_t12m_k"].max()
    ax.set_ylim(0, max_h * 1.18)
    inside_threshold = max_h * 0.15
    outside_offset = max(max_h * 0.015, 60)

    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        sales_label = f"${row['sales_t12m_k']:,.0f}k"
        yoy_val = row.get("t12m_yoy_pct")
        yoy_label = "n/a" if pd.isna(yoy_val) else f"{yoy_val:+.1f}%"
        annotation = f"{sales_label}\n{yoy_label}"
        _annotate_vbar(ax, bar, annotation, max_h, inside_threshold, outside_offset)

    grand_total_rows = df_detail[df_detail["row_type"] == "grand_total"].copy()
    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_text = f"Category Total: ${gt_k:,.0f}k | {gt_yoy_label}"
    else:
        gt_text = "Category Total: n/a"

    ax.text(0.5, 0.96, gt_text, transform=ax.transAxes, ha="center", va="center", fontsize=10, fontweight="bold", color="black",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9))

    plt.tight_layout()
    plt.show()


def plot_item_chart(df_item: pd.DataFrame, category_name: str, month_start: str = "", top_n: int = 10) -> None:
    """Vertical bar chart of top N item names + Other with sales/YoY annotations."""
    item_rows = df_item[df_item["row_type"] == "detail"].copy()

    if len(item_rows) == 0:
        print("No item rows returned for current filters.")
        return

    item_rows = item_rows.sort_values("sales_t12m", ascending=False)
    top = item_rows.head(top_n).copy()
    rest = item_rows.iloc[top_n:].copy()

    if len(rest) > 0:
        other = _build_other_row(rest, "item_name", "Other")
        other["vendor_name"] = "Other Vendors"
        plot_df = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
    else:
        plot_df = top.copy()

    plot_df["item_name"] = plot_df["item_name"].astype(str)
    plot_df["vendor_name"] = plot_df.get("vendor_name", "Unknown Vendor").fillna("Unknown Vendor").astype(str)
    plot_df["sales_t12m_k"] = plot_df["sales_t12m"] / 1000.0

    top_vendor_order = plot_df.loc[plot_df["item_name"] != "Other", "vendor_name"].drop_duplicates().tolist()
    vendor_palette = plt.cm.tab20.colors
    vendor_colors = {vendor: vendor_palette[i % len(vendor_palette)] for i, vendor in enumerate(top_vendor_order)}
    vendor_colors["Other Vendors"] = "#7f7f7f"
    colors = [vendor_colors.get(v, "#7f7f7f") for v in plot_df["vendor_name"]]

    fig, ax = plt.subplots(figsize=(13, 7))
    bars = ax.bar(range(len(plot_df)), plot_df["sales_t12m_k"], color=colors)

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"Top Items (Top {top_n} + Other) - {category_name}{subtitle}")
    ax.set_xlabel("Item Name")
    ax.set_ylabel("Sales T12M ($k)")

    wrapped_labels = [textwrap.fill(name, width=14) for name in plot_df["item_name"]]
    ax.set_xticks(range(len(plot_df)))
    ax.set_xticklabels(wrapped_labels, rotation=0, ha="center", fontsize=8)

    legend_order = top_vendor_order + (["Other Vendors"] if "Other Vendors" in plot_df["vendor_name"].values else [])
    legend_handles = [mpatches.Patch(color=vendor_colors[v], label=v) for v in legend_order]
    ax.legend(
        handles=legend_handles,
        title="Vendor",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.90),
        ncol=min(4, max(1, len(legend_handles))),
        frameon=True,
    )

    max_h = plot_df["sales_t12m_k"].max()
    ax.set_ylim(0, max_h * 1.18)
    inside_threshold = max_h * 0.15
    outside_offset = max(max_h * 0.015, 60)

    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        sales_label = f"${row['sales_t12m_k']:,.0f}k"
        yoy_val = row.get("t12m_yoy_pct")
        yoy_label = "n/a" if pd.isna(yoy_val) else f"{yoy_val:+.1f}%"
        annotation = f"{sales_label}\n{yoy_label}"
        _annotate_vbar(ax, bar, annotation, max_h, inside_threshold, outside_offset)

    grand_total_rows = df_item[df_item["row_type"] == "grand_total"].copy()
    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_text = f"Category Total: ${gt_k:,.0f}k | {gt_yoy_label}"
    else:
        gt_text = "Category Total: n/a"

    ax.text(0.5, 0.96, gt_text, transform=ax.transAxes, ha="center", va="center", fontsize=10, fontweight="bold", color="black",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9))

    plt.tight_layout()
    plt.show()


def plot_category_item_style_chart(
    df_category: pd.DataFrame,
    category_family: str,
    month_start: str = "",
    top_n: int = 10,
) -> None:
    """Vertical bar chart of top N categories + Other within a family, styled like the item chart."""
    detail_rows = df_category[df_category["row_type"] == 0].copy()

    if len(detail_rows) == 0:
        print("No category rows returned for current filters.")
        return

    detail_rows = detail_rows.sort_values("sales_t12m", ascending=False)
    top = detail_rows.head(top_n).copy()
    rest = detail_rows.iloc[top_n:].copy()

    if len(rest) > 0:
        other = _build_other_row(rest, "category_name", "Other")
        plot_df = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
    else:
        plot_df = top.copy()

    plot_df["category_name"] = plot_df["category_name"].astype(str)
    plot_df["sales_t12m_k"] = plot_df["sales_t12m"] / 1000.0

    fig, ax = plt.subplots(figsize=(13, 7))
    palette = plt.cm.tab20.colors
    colors = [palette[i % len(palette)] for i in range(len(plot_df))]
    bars = ax.bar(range(len(plot_df)), plot_df["sales_t12m_k"], color=colors)

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"Top Categories (Top {top_n} + Other) - {category_family}{subtitle}")
    ax.set_xlabel("Category Name")
    ax.set_ylabel("Sales T12M ($k)")

    wrapped_labels = [textwrap.fill(name, width=16) for name in plot_df["category_name"]]
    ax.set_xticks(range(len(plot_df)))
    ax.set_xticklabels(wrapped_labels, rotation=0, ha="center", fontsize=8)

    max_h = plot_df["sales_t12m_k"].max()
    ax.set_ylim(0, max_h * 1.18)
    inside_threshold = max_h * 0.15
    outside_offset = max(max_h * 0.015, 60)

    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        sales_label = f"${row['sales_t12m_k']:,.0f}k"
        yoy_val = row.get("t12m_yoy_pct")
        yoy_label = "n/a" if pd.isna(yoy_val) else f"{yoy_val:+.1f}%"
        annotation = f"{sales_label}\n{yoy_label}"
        _annotate_vbar(ax, bar, annotation, max_h, inside_threshold, outside_offset)

    grand_total_rows = df_category[df_category["row_type"] == 1].copy()
    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_text = f"Family Total: ${gt_k:,.0f}k | {gt_yoy_label}"
    else:
        gt_text = "Family Total: n/a"

    ax.text(
        0.5,
        0.96,
        gt_text,
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=10,
        fontweight="bold",
        color="black",
        bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9),
    )

    plt.tight_layout()
    plt.show()


def plot_chain_chart(df_chain: pd.DataFrame, category_name: str, month_start: str = "", top_n: int = 10) -> None:
    """Vertical bar chart of top N chains + Other with sales/YoY annotations."""
    chain_rows = df_chain[df_chain["row_type"] == "detail"].copy()

    if len(chain_rows) == 0:
        print("No chain rows returned for current filters.")
        return

    chain_rows = chain_rows.sort_values("sales_t12m", ascending=False)

    # Keep Independent out of the ranked top bars and roll it into the ending bucket.
    independent_mask = chain_rows["chain"].astype(str).str.strip().str.upper() == "INDEPENDENT"
    independent_rows = chain_rows[independent_mask].copy()
    non_independent_rows = chain_rows[~independent_mask].copy()

    top = non_independent_rows.head(top_n).copy()
    rest = non_independent_rows.iloc[top_n:].copy()
    tail_bucket_rows = pd.concat([rest, independent_rows], ignore_index=True)

    if len(tail_bucket_rows) > 0:
        other = _build_other_row(tail_bucket_rows, "chain", "Independent + Other")
        plot_df = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
    else:
        plot_df = top.copy()

    plot_df["chain"] = plot_df["chain"].astype(str)
    plot_df["sales_t12m_k"] = plot_df["sales_t12m"] / 1000.0

    fig, ax = plt.subplots(figsize=(13, 7))
    palette = plt.cm.tab10.colors
    colors = [palette[i % len(palette)] for i in range(len(plot_df))]
    bars = ax.bar(range(len(plot_df)), plot_df["sales_t12m_k"], color=colors)

    subtitle = f" — T12M from {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"Top Chains (Top {top_n} + Other) - {category_name}{subtitle}")
    ax.set_xlabel("Chain")
    ax.set_ylabel("Sales T12M ($k)")

    wrapped_labels = [textwrap.fill(name, width=16) for name in plot_df["chain"]]
    ax.set_xticks(range(len(plot_df)))
    ax.set_xticklabels(wrapped_labels, rotation=0, ha="center", fontsize=8)

    max_h = plot_df["sales_t12m_k"].max()
    ax.set_ylim(0, max_h * 1.18)
    inside_threshold = max_h * 0.15
    outside_offset = max(max_h * 0.015, 60)

    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        sales_label = f"${row['sales_t12m_k']:,.0f}k"
        yoy_val = row.get("t12m_yoy_pct")
        yoy_label = "n/a" if pd.isna(yoy_val) else f"{yoy_val:+.1f}%"
        annotation = f"{sales_label}\n{yoy_label}"
        _annotate_vbar(ax, bar, annotation, max_h, inside_threshold, outside_offset)

    grand_total_rows = df_chain[df_chain["row_type"] == "grand_total"].copy()
    if len(grand_total_rows) > 0:
        gt = grand_total_rows.iloc[0]
        gt_k = gt["sales_t12m"] / 1000.0
        gt_yoy = gt.get("t12m_yoy_pct")
        gt_yoy_label = "n/a" if pd.isna(gt_yoy) else f"{gt_yoy:+.1f}%"
        gt_text = f"Category Total: ${gt_k:,.0f}k | {gt_yoy_label}"
    else:
        gt_text = "Category Total: n/a"

    ax.text(0.5, 0.96, gt_text, transform=ax.transAxes, ha="center", va="center", fontsize=10, fontweight="bold", color="black",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="0.6", alpha=0.9))

    plt.tight_layout()
    plt.show()


def plot_vendor_category_monthly_trend(
    df_trend: pd.DataFrame,
    scope_label: str,
    window_start: str = "",
    window_end: str = "",
    smoothing_months: int = 3,
) -> None:
    """Line chart of monthly sales for the top vendors in the selected scope."""
    if len(df_trend) == 0:
        print("No monthly vendor trend rows returned for current filters.")
        return

    plot_df = df_trend.copy()
    plot_df["sales_month"] = pd.to_datetime(plot_df["sales_month"])
    plot_df["vendor_label"] = plot_df["vendor_name"].map(_display_vendor_name)

    vendor_order = (
        plot_df[["vendor_rank", "vendor_label"]]
        .drop_duplicates()
        .sort_values("vendor_rank")
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    subtitle = ""
    if window_start and window_end:
        start_ts = pd.to_datetime(window_start)
        end_ts = pd.to_datetime(window_end)
        plot_df = plot_df[(plot_df["sales_month"] >= start_ts) & (plot_df["sales_month"] <= end_ts)].copy()
        subtitle = f" — {pd.to_datetime(window_start).strftime('%b %Y')} to {pd.to_datetime(window_end).strftime('%b %Y')}"

    for _, vendor_row in vendor_order.iterrows():
        vendor_label = vendor_row["vendor_label"]
        vendor_df = plot_df[plot_df["vendor_label"] == vendor_label].copy()
        vendor_df = (
            vendor_df.groupby("sales_month", as_index=False)["sales_dollars"]
            .sum()
            .sort_values("sales_month")
        )
        if smoothing_months > 1:
            vendor_df["sales_dollars"] = (
                vendor_df["sales_dollars"]
                .rolling(window=smoothing_months, min_periods=1)
                .mean()
            )
        ax.plot(
            vendor_df["sales_month"],
            vendor_df["sales_dollars"],
            marker="o",
            linewidth=2,
            label=vendor_label,
        )

    title = f"Monthly Sales for Top Vendors - {scope_label}{subtitle}"
    if smoothing_months > 1:
        title = f"{title} ({smoothing_months}-Month Rolling Avg)"
    ax.set_title(title)
    ax.set_xlabel("Sales Month")
    ax.set_ylabel("Monthly Sales")
    ax.tick_params(axis="x", rotation=45)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Vendor", loc="upper left")

    plt.tight_layout()
    plt.show()


SKU_TIER_VOLUME_ORDER = [
    "Tier 1: Core (Top 80% Volume)",
    "Tier 2: Niche (Next 15% Volume)",
    "Tier 3: Zombie (Bottom 5% Volume)",
]

SKU_TIER_REVENUE_ORDER = [
    "Tier 1: Core (Top 80% Revenue)",
    "Tier 2: Niche (Next 15% Revenue)",
    "Tier 3: Zombie (Bottom 5% Revenue)",
]

SKU_TIER_COLORS = ["#2196F3", "#FF9800", "#F44336"]


def plot_sku_tier_chart(
    df: pd.DataFrame,
    dimension: str,
    month_start: str = "",
    trailing_weeks: int = 12,
) -> None:
    """Horizontal paired bar chart comparing catalog share vs volume/revenue share by SKU tier.

    dimension: 'volume' or 'revenue'
    """
    import matplotlib.ticker as mtick
    import numpy as np

    if dimension == "volume":
        tier_order = SKU_TIER_VOLUME_ORDER
        share_col = "pct_of_volume"
        share_label = "% of Volume (Units Sold)"
        metric_label = "volume"
    else:
        tier_order = SKU_TIER_REVENUE_ORDER
        share_col = "pct_of_revenue"
        share_label = "% of Revenue"
        metric_label = "revenue"

    plot_df = df[df["tier_dimension"] == dimension].copy()
    plot_df["sku_tier"] = pd.Categorical(plot_df["sku_tier"], categories=tier_order, ordered=True)
    plot_df = plot_df.sort_values("sku_tier")
    plot_df["pct_catalog"] = plot_df["pct_of_catalog"] * 100
    plot_df["pct_share"] = plot_df[share_col] * 100

    y = np.arange(len(plot_df))
    bar_h = 0.35

    fig, ax = plt.subplots(figsize=(12, 5))

    bars_catalog = ax.barh(
        y + bar_h / 2, plot_df["pct_catalog"], height=bar_h,
        color="#607D8B",
    )
    bars_share = ax.barh(
        y - bar_h / 2, plot_df["pct_share"], height=bar_h,
        color=[SKU_TIER_COLORS[i] for i in range(len(plot_df))],
    )

    ax.set_yticks(y)
    ax.set_yticklabels(plot_df["sku_tier"])
    ax.invert_yaxis()
    ax.set_xlabel("Share (%)")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_xlim(0, max(plot_df["pct_catalog"].max(), plot_df["pct_share"].max()) * 1.45)

    subtitle = f" — T{trailing_weeks}W to {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"SKU Tier Distribution: Catalog Share vs {metric_label.title()} Share{subtitle}")

    x_max = ax.get_xlim()[1]
    for b, sku_count in zip(bars_catalog, plot_df["sku_count"]):
        w = b.get_width()
        ax.text(w + x_max * 0.01, b.get_y() + b.get_height() / 2,
                f"{w:.1f}%  % of Catalog ({int(sku_count):,} SKUs)", va="center", fontsize=8.5, color="#333333")
    for b in bars_share:
        w = b.get_width()
        ax.text(w + x_max * 0.01, b.get_y() + b.get_height() / 2,
                f"{w:.1f}%  % of {metric_label.title()}", va="center", fontsize=8.5, color="#333333")

    plt.tight_layout()
    plt.show()


TIER_ORDER_SHORT = ["Tier 1: Core", "Tier 2: Niche", "Tier 3: Zombie"]
TIER_SHORT_LABELS = ["Core\n(Top 80%)", "Niche\n(Next 15%)", "Zombie\n(Bottom 5%)"]


def plot_sku_tier_matrix(
    df: pd.DataFrame,
    month_start: str = "",
    trailing_weeks: int = 12,
) -> None:
    """3x3 heatmap matrix of volume tier vs revenue tier showing SKU count and revenue per cell.

    Diagonal cells are aligned tiers; off-diagonal cells reveal cross-tier divergence.
    """
    import numpy as np

    pivot_sku = df.pivot_table(
        index="volume_tier", columns="revenue_tier",
        values="sku_count", aggfunc="sum", fill_value=0,
    ).reindex(index=TIER_ORDER_SHORT, columns=TIER_ORDER_SHORT, fill_value=0)

    pivot_rev = df.pivot_table(
        index="volume_tier", columns="revenue_tier",
        values="revenue", aggfunc="sum", fill_value=0,
    ).reindex(index=TIER_ORDER_SHORT, columns=TIER_ORDER_SHORT, fill_value=0)

    fig, ax = plt.subplots(figsize=(8, 6))

    # Background heatmap by SKU count
    data = pivot_sku.values.astype(float)
    max_val = data.max() if data.max() > 0 else 1
    normalized = data / max_val

    # Color: blue for diagonal (aligned), orange for off-diagonal (divergent)
    for i in range(3):
        for j in range(3):
            if i == j:
                color = plt.cm.Blues(0.2 + normalized[i, j] * 0.6)
            else:
                color = plt.cm.Oranges(0.15 + normalized[i, j] * 0.7) if normalized[i, j] > 0 else "#F5F5F5"
            ax.add_patch(plt.Rectangle((j - 0.5, i - 0.5), 1, 1, color=color))

    # Cell annotations
    for i in range(3):
        for j in range(3):
            sku_n = int(pivot_sku.values[i, j])
            rev = pivot_rev.values[i, j]
            if sku_n == 0:
                continue
            rev_label = f"${rev / 1000:,.0f}k" if rev >= 1000 else f"${rev:,.0f}"
            text_color = "white" if normalized[i, j] > 0.5 else "#222222"
            ax.text(j, i, f"{sku_n:,} SKUs\n{rev_label}",
                    ha="center", va="center", fontsize=9,
                    fontweight="bold", color=text_color)

    ax.set_xticks(range(3))
    ax.set_xticklabels(TIER_SHORT_LABELS, fontsize=9)
    ax.set_yticks(range(3))
    ax.set_yticklabels(TIER_SHORT_LABELS, fontsize=9)
    ax.set_xlabel("Revenue Tier", fontsize=10, labelpad=10)
    ax.set_ylabel("Volume Tier", fontsize=10, labelpad=10)
    ax.set_xlim(-0.5, 2.5)
    ax.set_ylim(-0.5, 2.5)
    ax.invert_yaxis()

    # Diagonal label
    ax.text(2.48, -0.48, "← aligned", fontsize=7.5, color="#1565C0",
            ha="right", va="top", style="italic")
    ax.text(0.5, 2.48, "divergent →", fontsize=7.5, color="#E65100",
            ha="left", va="bottom", style="italic")

    subtitle = f" — T{trailing_weeks}W to {_month_label(month_start)}" if month_start else ""
    ax.set_title(f"SKU Tier Alignment: Volume vs Revenue{subtitle}\n"
                 f"Diagonal = aligned tiers  |  Off-diagonal = divergent SKUs", fontsize=10)

    plt.tight_layout()
    plt.show()


def display_sku_table(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    rename: dict[str, str] | None = None,
    currency_cols: list[str] | None = None,
    int_cols: list[str] | None = None,
):
    """Return a styled DataFrame for display in a Jupyter notebook.

    Defaults are tuned for the divergent SKU table (item, category, ASP, revenue, units, tiers)
    but all parameters can be overridden for other use cases.

    Parameters
    ----------
    columns : column subset to display (in order); defaults to all columns
    rename : display name overrides; defaults to standard SKU table labels
    currency_cols : columns to format as {:,.2f}; defaults to ['avg_selling_price']
    int_cols : columns to format as {:,}; defaults to ['total_revenue', 'total_units_sold']
    """
    _default_columns = [
        'item_description', 'category_family', 'category_name',
        'avg_selling_price', 'total_revenue', 'total_units_sold',
        'volume_tier', 'revenue_tier',
    ]
    _default_rename = {
        'item_description':  'Item',
        'category_family':   'Category Family',
        'category_name':     'Category',
        'avg_selling_price': 'ASP ($)',
        'total_revenue':     'Revenue ($)',
        'total_units_sold':  'Units Sold',
        'volume_tier':       'Volume Tier',
        'revenue_tier':      'Revenue Tier',
    }
    _default_currency_cols = ['ASP ($)']
    _default_int_cols = ['Revenue ($)', 'Units Sold']

    cols = columns if columns is not None else [c for c in _default_columns if c in df.columns]
    out = df[cols].rename(columns=rename if rename is not None else _default_rename)

    display_rename = rename if rename is not None else _default_rename
    cur_cols = currency_cols if currency_cols is not None else [
        display_rename.get(c, c) for c in (cols if columns is not None else _default_columns)
        if display_rename.get(c, c) in _default_currency_cols
    ]
    i_cols = int_cols if int_cols is not None else [
        display_rename.get(c, c) for c in (cols if columns is not None else _default_columns)
        if display_rename.get(c, c) in _default_int_cols
    ]

    fmt = {c: '{:,.2f}' for c in cur_cols}
    fmt.update({c: '{:,.0f}' for c in i_cols})

    return (
        out.style
        .format(fmt)
        .set_properties(**{'text-align': 'left'})
        .set_table_styles([{
            'selector': 'th',
            'props': [('text-align', 'left'), ('white-space', 'nowrap')],
        }])
        .hide(axis='index')
    )


def plot_sku_tier_by_category(
    df: pd.DataFrame,
    dimension: str,
    month_start: str = "",
    trailing_weeks: int = 12,
    top_n: int = 15,
) -> None:
    """100% stacked horizontal bar chart of SKU tier distribution by category family.

    Each bar is a category family. Segments show Core / Niche / Zombie share of
    that category's SKU catalog. Sorted by Zombie share descending so the most
    bloated categories appear at the top.

    dimension: 'volume' or 'revenue'
    top_n: limit to the N categories with the most SKUs (keeps chart readable).
    """
    import matplotlib.ticker as mtick
    import numpy as np

    metric_label = "Volume" if dimension == "volume" else "Revenue"

    plot_df = df[df["tier_dimension"] == dimension].copy()

    # Limit to top_n categories by total SKU count
    cat_totals = (
        plot_df.groupby("category_family")["sku_count"].sum()
        .nlargest(top_n)
        .index
    )
    plot_df = plot_df[plot_df["category_family"].isin(cat_totals)]

    # Pivot to wide: rows = category_family, cols = tier
    pivot = plot_df.pivot_table(
        index="category_family", columns="sku_tier",
        values="pct_of_catalog", aggfunc="sum", fill_value=0,
    ).reindex(columns=TIER_ORDER_SHORT, fill_value=0)

    # Sort by Zombie share descending (most bloated at top when inverted)
    pivot = pivot.sort_values("Tier 3: Zombie", ascending=True)

    categories = pivot.index.tolist()
    y = np.arange(len(categories))
    bar_h = 0.55

    fig, ax = plt.subplots(figsize=(12, max(5, len(categories) * 0.55 + 1.5)))

    tier_labels = ["Core (Tier 1)", "Niche (Tier 2)", "Zombie (Tier 3)"]
    left = np.zeros(len(categories))

    for tier, color, label in zip(TIER_ORDER_SHORT, SKU_TIER_COLORS, tier_labels):
        values = pivot[tier].values * 100
        bars = ax.barh(y, values, height=bar_h, left=left, color=color, label=label)
        for b, v, lo in zip(bars, values, left):
            if v >= 6:
                ax.text(
                    lo + v / 2, b.get_y() + b.get_height() / 2,
                    f"{v:.0f}%", ha="center", va="center",
                    fontsize=8, color="white", fontweight="bold",
                )
        left += values

    ax.set_yticks(y)
    ax.set_yticklabels(categories, fontsize=9)
    ax.set_xlabel("Share of Category SKU Catalog (%)")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())

    # Annotate zombie SKU count at the far right
    zombie_counts = plot_df[plot_df["sku_tier"] == "Tier 3: Zombie"].set_index("category_family")["sku_count"]
    total_counts = plot_df.groupby("category_family")["sku_count"].sum()
    for i, cat in enumerate(categories):
        z = int(zombie_counts.get(cat, 0))
        t = int(total_counts.get(cat, 0))
        ax.text(
            101, i, f"{z:,} / {t:,} SKUs",
            va="center", fontsize=7.5, color="#555555",
        )

    ax.legend(
        loc="upper center", bbox_to_anchor=(0.42, -0.08),
        ncol=3, fontsize=8.5, frameon=False,
    )
    subtitle = f" — T{trailing_weeks}W to {_month_label(month_start)}" if month_start else ""
    ax.set_title(
        f"SKU Tier Distribution by Category Family: {metric_label}{subtitle}\n"
        f"Sorted by Zombie share — Zombie / Total SKU count shown at right",
        fontsize=10,
    )
    ax.set_xlim(0, 118)

    plt.tight_layout()
    plt.show()


ARCHETYPE_COLORS = {
    "Collector / Ultra-Premium":  "#2196F3",
    "Seasonal / Limited Release": "#FF9800",
    "Mini Variety Pack":          "#4CAF50",
}

_SCATTER_LABEL_SKUS = {
    "JOHNNIE WALKER BLUE",
    "FOUR ROSES LIMITED EDITION 2025",
    "HENNESSY XO GB UPGRADE",
    "JOSEPH MAGNUS CIGAR BLEND BOURBON",
    "WOODFORD RESERVE BARREL STRENGTH RYE BARREL PROOF",
    "HIBIKI JAPANESE HARMONY",
    "MACALLAN 12YR",
    "HENNESSY VS",
    "99 CHERRIES MINI",
    "FIELD OF DREAMS ALL STAR WHISKEY",
}


def plot_sku_archetype_scatter(
    df: pd.DataFrame,
    month_start: str = "",
    trailing_weeks: int = 12,
) -> None:
    """Scatter plot of Core Revenue / Zombie Volume SKUs coloured by planning archetype.

    X = total_units_sold, Y = total_revenue, size = avg_selling_price.
    Annotates key SKUs by name.
    """
    import numpy as np
    import matplotlib.ticker as mtick

    plot_df = df.copy()

    # Size scaling: 80–400 range mapped to ASP range
    min_asp = plot_df["avg_selling_price"].min()
    max_asp = plot_df["avg_selling_price"].max()
    asp_range = max_asp - min_asp if max_asp > min_asp else 1.0
    plot_df["_dot_size"] = 80 + ((plot_df["avg_selling_price"] - min_asp) / asp_range) * 320

    fig, ax = plt.subplots(figsize=(12, 7))

    archetypes = ["Collector / Ultra-Premium", "Seasonal / Limited Release", "Mini Variety Pack"]
    for archetype in archetypes:
        mask = plot_df["archetype"] == archetype
        sub = plot_df[mask]
        if sub.empty:
            continue
        ax.scatter(
            sub["total_units_sold"],
            sub["total_revenue"],
            s=sub["_dot_size"],
            c=ARCHETYPE_COLORS[archetype],
            alpha=0.85,
            edgecolors="white",
            linewidths=0.6,
            label=archetype,
            zorder=3,
        )

    # SKU name annotations — fixed offsets per point to avoid overlap
    _label_offsets = {
        "JOHNNIE WALKER BLUE":                          ( 12,  1500),
        "FOUR ROSES LIMITED EDITION 2025":              ( 12,  1500),
        "HENNESSY XO GB UPGRADE":                       ( 12, -3500),
        "JOSEPH MAGNUS CIGAR BLEND BOURBON":            ( 12,  1500),
        "WOODFORD RESERVE BARREL STRENGTH RYE BARREL PROOF": ( 12,  1500),
        "HIBIKI JAPANESE HARMONY":                      ( 12,  1500),
        "MACALLAN 12YR":                                ( 12, -3000),
        "HENNESSY VS":                                  ( 12,  1500),
        "99 CHERRIES MINI":                             ( 12,  1500),
        "FIELD OF DREAMS ALL STAR WHISKEY":             ( 12, -3000),
    }
    for _, row in plot_df.iterrows():
        name = str(row.get("item_description", "")).upper().strip()
        matched_key = next((k for k in _SCATTER_LABEL_SKUS if k in name), None)
        if matched_key is None:
            continue
        display = str(row["item_description"])
        if len(display) > 32:
            display = display[:30] + "…"
        dx, dy = _label_offsets.get(matched_key, (12, 1500))
        ax.annotate(
            display,
            xy=(row["total_units_sold"], row["total_revenue"]),
            xytext=(row["total_units_sold"] + dx, row["total_revenue"] + dy),
            fontsize=7.5,
            color="#333333",
            arrowprops=dict(arrowstyle="-", color="#BDBDBD", lw=0.7),
            va="bottom",
        )

    # Collector cluster annotation — placed in empty left zone, well clear of Four Roses label
    ax.text(
        8, 29000,
        "Low units, high ASP\n→ Fragile premium",
        fontsize=8, color="#555555", style="italic", va="top",
    )

    # Axes
    ax.set_xlabel("Units Sold (T12W)", fontsize=10)
    ax.set_ylabel("Total Revenue ($)", fontsize=10)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.grid(axis="both", alpha=0.2, zorder=0)

    # Both legends placed inside the chart on the left where there are no data points
    archetype_legend = ax.legend(
        title="Archetype",
        loc="upper left",
        bbox_to_anchor=(0.01, 0.99),
        frameon=True,
        framealpha=0.9,
        fontsize=8.5,
        title_fontsize=9,
    )
    ax.add_artist(archetype_legend)

    # ASP size legend
    mid_asp = (min_asp + max_asp) / 2
    size_handles = []
    for asp_val, label in [
        (min_asp, f"${min_asp:,.0f} ASP"),
        (mid_asp,  f"${mid_asp:,.0f} ASP"),
        (max_asp,  f"${max_asp:,.0f} ASP"),
    ]:
        s = 80 + ((asp_val - min_asp) / asp_range) * 320
        size_handles.append(
            ax.scatter([], [], s=s, c="#BDBDBD", edgecolors="white", linewidths=0.6, label=label)
        )
    ax.legend(
        handles=size_handles,
        title="Avg Selling Price",
        loc="lower left",
        bbox_to_anchor=(0.01, 0.01),
        frameon=True,
        framealpha=0.9,
        fontsize=8.5,
        title_fontsize=9,
    )

    subtitle = f"T{trailing_weeks}W to {_month_label(month_start)}" if month_start else ""
    ax.set_title(
        f"Core Revenue / Zombie Volume SKUs — Three Planning Archetypes\n{subtitle}",
        fontsize=10,
    )

    plt.tight_layout()
    plt.show()


# Exact item_description matches (case-insensitive) for the non-Core/Core groups.
# Core/Core labels are chosen at render time from the top N by revenue to avoid
# substring explosion (e.g. "BLACK VELVET" matching every variant).
_FULL_SCATTER_LABEL_EXACT = {
    "JOHNNIE WALKER BLUE",
    "FOUR ROSES LIMITED EDITION 2025",
    "E & J VS PET",
    "99 CINNAMON",
}
_FULL_SCATTER_CORE_TOP_N = 3  # label this many Core/Core outliers by revenue

_FULL_SCATTER_GROUP_COLORS = {
    "Core / Core":              "#2196F3",
    "Fragile Premium":          "#FF9800",
    "High Volume / Low Revenue": "#F44336",
    "Zombie / Zombie":          "#B0B0B0",
    "Other":                    "#D0D0D0",
}


def plot_sku_full_scatter(
    df: pd.DataFrame,
    month_start: str = "",
    trailing_weeks: int = 12,
    log_scale: bool = True,
    category_family: str | None = None,
    isolation_threshold: float = 0.08,
) -> None:
    """Log-log (or linear) scatter of all active SKUs, with highlighted overlays for key groups.

    Background layers (Other, Zombie/Zombie) are unlabelled; highlighted groups
    (Core/Core, Fragile Premium, High Volume/Low Revenue) appear in the legend.

    log_scale: True (default) for log-log axes, False for linear axes.
    category_family: if provided, filter to only SKUs in that category family.
    """
    import numpy as np
    import matplotlib.ticker as mtick

    if category_family is not None:
        df = df[df["category_family"] == category_family].copy()
        if df.empty:
            print(f"No SKUs found for category_family={category_family!r}")
            return

    # Bucket bottle_volume_ml into human-readable size bands
    # Each band maps to a distinct marker shape
    _VOLUME_BUCKETS = [
        (50,   50,   "50ml",    "h"),   # hexagon — small/distinctive for minis
        (100,  100,  "100ml",   "X"),   # x-fill
        (200,  200,  "200ml",   "s"),   # square
        (375,  375,  "375ml",   "^"),   # triangle up
        (750,  750,  "750ml",   "o"),   # circle (most common — default)
        (1000, 1000, "1L",      "D"),   # diamond
        (1750, 1750, "1.75L",   "P"),   # plus — clearly distinct from circle
    ]
    _DEFAULT_MARKER = "o"

    def _get_marker(vol):
        if pd.isna(vol):
            return _DEFAULT_MARKER
        vol = int(vol)
        for lo, hi, _, mk in _VOLUME_BUCKETS:
            if lo <= vol <= hi:
                return mk
        return _DEFAULT_MARKER

    def _get_size_label(vol):
        if pd.isna(vol):
            return "Other"
        vol = int(vol)
        for lo, hi, label, _ in _VOLUME_BUCKETS:
            if lo <= vol <= hi:
                return label
        return f"{vol}ml"

    df = df.copy()
    df["_marker"] = df["bottle_volume_ml"].apply(_get_marker)
    df["_size_label"] = df["bottle_volume_ml"].apply(_get_size_label)

    fig, ax = plt.subplots(figsize=(12, 8))

    # Plot layers by group × marker combination so each marker is a separate scatter call.
    # Background groups (Other, Zombie/Zombie) are unlabelled.
    # Highlighted groups carry a legend entry on their first marker only.
    _layer_config = [
        ("Other",                    "#D0D0D0", 0.30, 15, 1, None),
        ("Zombie / Zombie",          "#B0B0B0", 0.25, 15, 2, None),
        ("Core / Core",              "#2196F3", 0.70, 40, 3, "Core / Core — True Anchors"),
        ("Fragile Premium",          "#FF9800", 0.90, 60, 4, "Core Revenue / Zombie Volume — Fragile Premium"),
        ("High Volume / Low Revenue","#F44336", 0.90, 60, 5, "Core Volume / Zombie Revenue — Low Price High Volume"),
    ]

    # Track which group labels have already been added to the legend
    _legend_added = set()

    for sg, color, alpha, size, zorder, legend_label in _layer_config:
        grp = df[df["scatter_group"] == sg]
        if grp.empty:
            continue
        for marker, sub in grp.groupby("_marker"):
            # Only attach the legend label to the first marker sub-group for this scatter_group
            use_label = None
            if legend_label and sg not in _legend_added:
                use_label = legend_label
                _legend_added.add(sg)
            ax.scatter(
                sub["total_units_sold"], sub["total_revenue"],
                s=size, c=color, alpha=alpha, zorder=zorder,
                marker=marker, linewidths=0.4,
                edgecolors="white" if size >= 40 else "none",
                label=use_label,
            )

    # Scale and axis formatting
    x_vals = df["total_units_sold"]
    y_vals = df["total_revenue"]

    if log_scale:
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("Units Sold — T12W (log scale)", fontsize=10)
        ax.set_ylabel("Total Revenue — T12W (log scale)", fontsize=10)
        ax.xaxis.set_major_formatter(mtick.FuncFormatter(
            lambda v, _: f"{int(v):,}" if v >= 1 else f"{v:.1f}"
        ))
        ax.grid(which="major", color="#CCCCCC", alpha=0.3, zorder=0)
        ax.grid(which="minor", color="#EEEEEE", alpha=0.3, zorder=0)
    else:
        ax.set_xlabel("Units Sold — T12W", fontsize=10)
        ax.set_ylabel("Total Revenue — T12W", fontsize=10)
        ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{int(v):,}"))
        ax.grid(which="major", color="#CCCCCC", alpha=0.3, zorder=0)

    ax.yaxis.set_major_formatter(mtick.FuncFormatter(
        lambda v, _: (
            f"${v/1_000_000:.0f}M" if v >= 1_000_000 else
            f"${v/1_000:.0f}k"    if v >= 1_000      else
            f"${v:.0f}"
        )
    ))

    # SKU label annotations — use log-space offsets when log scale, linear fractions otherwise
    if log_scale:
        x_min_ref, x_max_ref = np.log10(x_vals.min()), np.log10(x_vals.max())
        y_min_ref, y_max_ref = np.log10(y_vals.min()), np.log10(y_vals.max())
        x_range = x_max_ref - x_min_ref
        y_range = y_max_ref - y_min_ref

        def _label_pos(xu, yu, dy_frac):
            xl = np.log10(xu)
            yl = np.log10(yu)
            return 10 ** (xl + x_range * 0.018), 10 ** (yl + y_range * dy_frac)

        x_far_left  = 10 ** (x_min_ref + x_range * 0.01)
        x_far_right = 10 ** (x_min_ref + x_range * 0.78)
        y_top       = 10 ** (y_min_ref + y_range * 0.90)
        y_bottom    = 10 ** (y_min_ref + y_range * 0.04)
    else:
        x_min_ref, x_max_ref = x_vals.min(), x_vals.max()
        y_min_ref, y_max_ref = y_vals.min(), y_vals.max()
        x_range = x_max_ref - x_min_ref
        y_range = y_max_ref - y_min_ref

        def _label_pos(xu, yu, dy_frac):
            return xu + x_range * 0.005, yu + y_range * dy_frac

        x_far_left  = x_min_ref + x_range * 0.01
        x_far_right = x_min_ref + x_range * 0.72
        y_top       = y_min_ref + y_range * 0.90
        y_bottom    = y_min_ref + y_range * 0.04

    # Pick top N Core/Core by revenue for the explicit label allowlist
    core_label_items = (
        df[df["scatter_group"] == "Core / Core"]
        .nlargest(_FULL_SCATTER_CORE_TOP_N, "total_revenue")["item_description"]
        .str.upper().str.strip().tolist()
    )

    # Pre-compute log-space coordinates for all points (used for proximity check)
    _all_log_x = np.log10(df["total_units_sold"].clip(lower=1e-9).values)
    _all_log_y = np.log10(df["total_revenue"].clip(lower=1e-9).values)

    # Isolation threshold: a point is labelable if no other point is within this
    # normalised log-space distance (0 = identical position, 1 = full axis range apart).
    # 0.06 ≈ ~6% of the axis range — catches tight clusters, allows sparse outliers.
    _ISOLATION_THRESHOLD = isolation_threshold

    def _is_isolated(xu, yu):
        """Return True if the point has no neighbours within the isolation threshold."""
        if log_scale:
            pt_x = np.log10(max(xu, 1e-9))
            pt_y = np.log10(max(yu, 1e-9))
            norm_x = (pt_x - x_min_ref) / (x_range or 1)
            norm_y = (pt_y - y_min_ref) / (y_range or 1)
            all_nx = (_all_log_x - x_min_ref) / (x_range or 1)
            all_ny = (_all_log_y - y_min_ref) / (y_range or 1)
        else:
            norm_x = (xu - x_min_ref) / (x_range or 1)
            norm_y = (yu - y_min_ref) / (y_range or 1)
            all_nx = (df["total_units_sold"].values - x_min_ref) / (x_range or 1)
            all_ny = (df["total_revenue"].values - y_min_ref) / (y_range or 1)
        dists = np.sqrt((all_nx - norm_x) ** 2 + (all_ny - norm_y) ** 2)
        return np.sum(dists < _ISOLATION_THRESHOLD) <= 1

    from adjustText import adjust_text

    label_texts = []
    label_points_x = []
    label_points_y = []

    for _, row in df.iterrows():
        name = str(row.get("item_description", "")).upper().strip()
        in_allowlist = name in core_label_items or name in _FULL_SCATTER_LABEL_EXACT
        isolated = _is_isolated(row["total_units_sold"], row["total_revenue"])
        if not (in_allowlist or isolated):
            continue
        # Don't label background noise even if isolated — only highlight groups
        if not in_allowlist and row["scatter_group"] in ("Other", "Zombie / Zombie"):
            continue
        group = row["scatter_group"]
        color = _FULL_SCATTER_GROUP_COLORS.get(group, "#333333")
        size_label = _get_size_label(row.get("bottle_volume_ml"))
        display = f"{row['item_description']} ({size_label})"
        xu, yu = row["total_units_sold"], row["total_revenue"]
        t = ax.text(xu, yu, display, fontsize=8, color=color, va="bottom")
        label_texts.append(t)
        label_points_x.append(xu)
        label_points_y.append(yu)

    if label_texts:
        adjust_text(
            label_texts,
            x=label_points_x,
            y=label_points_y,
            ax=ax,
            arrowprops=dict(arrowstyle="-", color="#BDBDBD", lw=0.6),
            expand=(1.2, 1.4),
            force_points=(0.3, 0.5),
            force_text=(0.5, 0.8),
        )

    # Quadrant dividing lines at the geometric (log) or arithmetic midpoint of the data range
    if log_scale:
        x_mid = 10 ** ((x_min_ref + x_max_ref) / 2)
        y_mid = 10 ** ((y_min_ref + y_max_ref) / 2)
    else:
        x_mid = (x_min_ref + x_max_ref) / 2
        y_mid = (y_min_ref + y_max_ref) / 2

    ax.axvline(x_mid, color="#CCCCCC", linewidth=0.8, linestyle="--", zorder=0)
    ax.axhline(y_mid, color="#CCCCCC", linewidth=0.8, linestyle="--", zorder=0)

    # Quadrant annotations — placed using axes-fraction coordinates so they always
    # sit in the corners regardless of data range, with a light background box.
    _quadrant_annotations = [
        (0.98, 0.60, "Core anchors\nhigh volume + high revenue", "right", "top"),
        (0.02, 0.60, "Fragile premium\nhigh revenue, low volume", "left",  "top"),
        (0.98, 0.06, "High volume\nlow commercial return",         "right", "bottom"),
        (0.02, 0.06, "Long tail\nlow volume + low revenue",        "left",  "bottom"),
    ]
    for ax_x, ax_y, text, ha, va in _quadrant_annotations:
        ax.text(
            ax_x, ax_y, text,
            transform=ax.transAxes,
            fontsize=8, color="#333333", style="italic",
            ha=ha, va=va,
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="none", alpha=0.7),
        )

    # Group legend (color) — top left
    group_legend = ax.legend(
        loc="upper left",
        bbox_to_anchor=(0.01, 0.99),
        frameon=True,
        framealpha=0.9,
        fontsize=8.5,
        title_fontsize=9,
    )
    ax.add_artist(group_legend)

    # Size/shape legend — below the group legend, showing only sizes present in this data
    present_sizes = df["_size_label"].unique()
    shape_handles = []
    for lo, hi, label, marker in _VOLUME_BUCKETS:
        if label not in present_sizes:
            continue
        shape_handles.append(
            ax.scatter([], [], s=30, c="#888888", marker=marker,
                       linewidths=0, label=label)
        )
    if shape_handles:
        ax.legend(
            handles=shape_handles,
            title="Bottle Size",
            loc="upper left",
            bbox_to_anchor=(0.01, 0.90),
            frameon=True,
            framealpha=0.9,
            fontsize=8.5,
            title_fontsize=9,
        )

    scale_label = "Log scale" if log_scale else "Linear scale"
    scope = f"{category_family}" if category_family else "All active SKUs"
    subtitle = f"{scope} | T{trailing_weeks}W to {_month_label(month_start)} | {scale_label}" if month_start else f"{scope} | {scale_label}"
    ax.set_title(
        f"SKU Catalog Distribution — Volume vs. Revenue\n{subtitle}",
        fontsize=10,
    )

    plt.tight_layout()
    plt.show()


# ---------------------------------------------------------------------------
# Store performance charts
# ---------------------------------------------------------------------------

_CHAIN_COLORS: dict[str, str] = {
    "HY-VEE":    "#1f77b4",
    "CASEY'S":   "#ff7f0e",
    "FAREWAY":   "#2ca02c",
    "WALMART":   "#d62728",
    "SAM'S CLUB": "#9467bd",
    "Other":     "#aec7e8",
}
_DEFAULT_CHAIN_COLOR = "#cccccc"


def _chain_color(chain_group: str) -> str:
    return _CHAIN_COLORS.get(chain_group, _DEFAULT_CHAIN_COLOR)


def plot_chain_market_share(
    df: "pd.DataFrame",
    month_start: str,
    trailing_weeks: int,
) -> None:
    """Horizontal bar chart: total sales by chain group with % of market labels.

    df must have columns: chain_group, total_sales, pct_of_market.
    Rows should be ordered by total_sales descending (SQL already does this).
    """
    df_plot = df.sort_values("total_sales")

    colors = [_chain_color(c) for c in df_plot["chain_group"]]

    fig, ax = plt.subplots(figsize=(9, max(3, len(df_plot) * 0.55)))

    bars = ax.barh(df_plot["chain_group"], df_plot["total_sales"], color=colors)

    x_max = df_plot["total_sales"].max()
    for bar, (_, row) in zip(bars, df_plot.iterrows()):
        pct = row["pct_of_market"]
        val = row["total_sales"]
        label = _literal_currency(f"${val / 1_000_000:.1f}M  ({pct:.0%})")
        threshold = x_max * 0.35
        if val >= threshold:
            ax.text(val - x_max * 0.01, bar.get_y() + bar.get_height() / 2,
                    label, ha="right", va="center", fontsize=8.5, color="white", fontweight="bold")
        else:
            ax.text(val + x_max * 0.01, bar.get_y() + bar.get_height() / 2,
                    label, ha="left", va="center", fontsize=8.5, color="#333333")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: _literal_currency(f"${x / 1_000_000:.1f}M"))
    )
    ax.set_xlabel("Total Sales")
    ax.set_title(
        f"Chain Market Share — Total Sales\nT{trailing_weeks}W to {_month_label(month_start)}",
        fontsize=10,
    )

    plt.tight_layout()
    plt.show()


def plot_store_productivity(
    df: "pd.DataFrame",
    month_start: str,
    trailing_weeks: int,
) -> None:
    """Horizontal bar chart: avg sales per store by chain group.

    df must have columns: chain_group, avg_sales_per_store, store_count.
    """
    df_plot = df.sort_values("avg_sales_per_store")

    colors = [_chain_color(c) for c in df_plot["chain_group"]]

    fig, ax = plt.subplots(figsize=(9, max(3, len(df_plot) * 0.55)))

    bars = ax.barh(df_plot["chain_group"], df_plot["avg_sales_per_store"], color=colors)

    x_max = df_plot["avg_sales_per_store"].max()
    for bar, (_, row) in zip(bars, df_plot.iterrows()):
        val = row["avg_sales_per_store"]
        n = int(row["store_count"])
        label = _literal_currency(f"${val / 1_000:,.0f}K  (n={n})")
        threshold = x_max * 0.45
        if val >= threshold:
            ax.text(val - x_max * 0.01, bar.get_y() + bar.get_height() / 2,
                    label, ha="right", va="center", fontsize=8.5, color="white", fontweight="bold")
        else:
            ax.text(val + x_max * 0.01, bar.get_y() + bar.get_height() / 2,
                    label, ha="left", va="center", fontsize=8.5, color="#333333")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: _literal_currency(f"${x / 1_000:,.0f}K"))
    )
    ax.set_xlabel("Avg Sales per Store")
    ax.set_title(
        f"Store Productivity — Avg Sales per Store\nT{trailing_weeks}W to {_month_label(month_start)}",
        fontsize=10,
    )

    plt.tight_layout()
    plt.show()


def plot_channel_mix(
    df: "pd.DataFrame",
    month_start: str,
    trailing_weeks: int,
) -> None:
    """Horizontal bar chart: total sales by store channel with % and store count labels.

    df must have columns: store_channel, total_sales, pct_of_market, store_count.
    """
    df_plot = df.sort_values("total_sales")

    fig, ax = plt.subplots(figsize=(9, max(3, len(df_plot) * 0.55)))

    bars = ax.barh(df_plot["store_channel"], df_plot["total_sales"], color="#5b9bd5")

    x_max = df_plot["total_sales"].max()
    for bar, (_, row) in zip(bars, df_plot.iterrows()):
        val = row["total_sales"]
        pct = row["pct_of_market"]
        n = int(row["store_count"])
        label = _literal_currency(f"${val / 1_000_000:.1f}M  ({pct:.0%}, n={n})")
        threshold = x_max * 0.45
        if val >= threshold:
            ax.text(val - x_max * 0.01, bar.get_y() + bar.get_height() / 2,
                    label, ha="right", va="center", fontsize=8.5, color="white", fontweight="bold")
        else:
            ax.text(val + x_max * 0.01, bar.get_y() + bar.get_height() / 2,
                    label, ha="left", va="center", fontsize=8.5, color="#333333")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: _literal_currency(f"${x / 1_000_000:.1f}M"))
    )
    ax.set_xlabel("Total Sales")
    ax.set_title(
        f"Store Channel Mix — Total Sales by Retail Format\nT{trailing_weeks}W to {_month_label(month_start)}",
        fontsize=10,
    )

    plt.tight_layout()
    plt.show()
