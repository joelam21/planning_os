from __future__ import annotations

import textwrap
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
        f"{vendor_number}\n{textwrap.fill(str(vendor_name), width=14)}"
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
        f"{vendor_number}\n{textwrap.fill(str(vendor_name), width=14)}"
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
        f"{vendor_number}\n{textwrap.fill(str(vendor_name), width=14)}"
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
            asp_str = "n/a" if pd.isna(ch_asp) else f"\${ ch_asp:,.2f}"
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
            f"{vendor_number}\n{textwrap.fill(str(vendor_name), width=14)}"
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
            "Other Vendors" if row["vendor_number"] == "Other" else f"{row['vendor_number']} - {row['vendor_name']}"
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
            f"{vendor_number}\n{textwrap.fill(str(vendor_name), width=14)}"
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
    plot_df["vendor_label"] = plot_df["vendor_name"].astype(str) + " (" + plot_df["vendor_number"].astype(str) + ")"

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
