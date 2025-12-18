import io
import os
import pandas as pd
import streamlit as st
from datetime import datetime

# ---------------- Config ----------------
STOCK = 4700.0
MIN_REUSE = 330.0            # strictly > 330 mm is reusable
MEMORY_FILE = "offcut_memory.csv"  # local CSV for persistent offcuts
APP_TITLE = "🪚 Cut Batch Optimizer (with Offcut Memory)"
# ---------------------------------------

st.set_page_config(page_title="Cut Batch Optimizer", page_icon="🪚", layout="centered")
st.title(APP_TITLE)
st.caption("Upload XLS/XLSX/CSV. Optimizes groups with offcut reuse and remembers reusable offcuts for future batches.")

# ---------------------
# Machine-readable format converter
# ---------------------
def transform_optimized_to_machine_readable(optimized_df: pd.DataFrame) -> pd.DataFrame:
    """Convert the optimized output DataFrame into the machine-readable profile CSV layout.

    Notes (as requested):
    - Uses `optimized_group` as the group/page column.
    - Uses `itemId` as the barcode/item id column.
    - Reads from the optimized sheet/output (NOT the original uploaded batch).
    """

    df = optimized_df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # Trim string cells
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    def find_col(*names: str) -> str | None:
        lowered = {str(c).strip().lower(): c for c in df.columns}
        for name in names:
            key = str(name).strip().lower()
            if key in lowered:
                return lowered[key]
        return None

    col_group = find_col("optimized_group")
    col_material = find_col("material")
    col_length = find_col("length")
    col_qty = find_col("qty", "quantity")
    col_itemid = find_col("itemId", "itemid", "item_id", "item id")

    if not col_group:
        raise ValueError("Optimized output is missing required column: optimized_group")
    if not col_material:
        raise ValueError("Optimized output is missing required column: material")
    if not col_length:
        raise ValueError("Optimized output is missing required column: length")
    if not col_itemid:
        raise ValueError("Optimized output is missing required column: itemId")

    # If qty is missing, treat each row as 1
    if not col_qty:
        df["_qty"] = "1"
        col_qty = "_qty"

    # Preserve group order as first occurrence
    first_rows = df.groupby(col_group, sort=False).head(1).reset_index(drop=True)
    group_order = list(first_rows[col_group].astype(str))

    # Stop BEFORE first group whose first row material starts with FCT
    stop_at_idx = None
    for idx, g in enumerate(group_order):
        gdf = df[df[col_group].astype(str) == str(g)]
        if not gdf.empty:
            mat = str(gdf.iloc[0][col_material] or "").upper()
            if mat.startswith("FCT"):
                stop_at_idx = idx
                break
    kept_groups = group_order[:stop_at_idx] if stop_at_idx is not None else group_order

    group_rows = {str(g): df[df[col_group].astype(str) == str(g)].reset_index(drop=True) for g in kept_groups}
    num_pages = len(kept_groups)
    max_items = max((len(gdf) for gdf in group_rows.values()), default=0)

    def get_val(g: str, idx: int, col: str | None, default: str = "") -> str:
        gdf = group_rows[str(g)]
        if idx < len(gdf) and col and col in gdf.columns:
            v = gdf.iloc[idx][col]
            return "" if pd.isna(v) else str(v)
        return default

    rows: list[list[str]] = []
    rows.append(["List separator=", "Decimal symbol=."] + [""] * max(0, num_pages - 2))
    rows.append(["Scheme Scheme"] + [""] * num_pages)
    page_labels = [f"Page_{i+1}" for i in range(num_pages)]
    rows.append(["LANGID_804"] + page_labels)
    rows.append(["LANGID_404"] + page_labels)
    rows.append(["1"] + [str(i + 1) for i in range(num_pages)])

    rows.append(["204_HMI_Scheme_ProjectData_BarchCode"] + ["1"] * num_pages)
    rows.append(["204_HMI_Scheme_ProjectData_EngInfo"] + ["1"] * num_pages)
    rows.append(["204_HMI_Scheme_ProjectData_ProfileName"] + [get_val(g, 0, col_material, "") for g in kept_groups])
    rows.append(["204_HMI_Scheme_ProjectData_ProfileCode"] + ["0"] * num_pages)
    rows.append(["204_HMI_Scheme_ProjectData_RawLength"] + ["4870"] * num_pages)
    rows.append(["204_HMI_Scheme_ProjectData_RawHeight"] + ["0"] * num_pages)
    rows.append(["204_HMI_Scheme_ProjectData_RawWidth"] + ["0"] * num_pages)
    rows.append(["204_HMI_Scheme_ProjectData_Amount"] + ["0"] * num_pages)

    for k in range(1, max_items + 1):
        idx = k - 1
        rows.append([f"204_HMI_Scheme_ProjectData_PerformData{{{k}}}.length"]
                    + [(get_val(g, idx, col_length, "0") or "0") for g in kept_groups])
        rows.append([f"204_HMI_Scheme_ProjectData_PerformData{{{k}}}.angle2"] + ["0"] * num_pages)
        rows.append([f"204_HMI_Scheme_ProjectData_PerformData{{{k}}}.angle1"] + ["0"] * num_pages)
        rows.append([f"204_HMI_Scheme_ProjectData_PerformData{{{k}}}.quantity"]
                    + [(get_val(g, idx, col_qty, "1") or "1") for g in kept_groups])

    for k in range(1, max_items + 1):
        idx = k - 1
        rows.append([f"204_HMI_Scheme_ProjectData_PerformData{{{k}}}.barcode"]
                    + [get_val(g, idx, col_itemid, "") for g in kept_groups])

    return pd.DataFrame(rows).fillna("")


def _extract_material_prefix(material_value) -> str:
    if material_value is None or (isinstance(material_value, float) and pd.isna(material_value)):
        return ""
    s = str(material_value).strip()
    if not s:
        return ""
    return s.split(".")[0].strip()


def reorder_barcode_pdf_to_optimized(pdf_bytes: bytes, optimized_out: pd.DataFrame) -> tuple[io.BytesIO, pd.DataFrame]:
    """Reorder barcode PDF pages to match optimized cut order.

    Assumptions based on your description:
    - Original barcode PDF pages are in the SAME sequence as the original cut-batch rows.
    - We align pages to original rows by matching material prefix (e.g., 3223) to barcode text token like P3223....
    - If a PDF page doesn't contain a material code (no P####), it is skipped during alignment.
    - Final output is reordered to match the optimized order (optimized_group, then orig_index).
    """

    try:
        import fitz  # PyMuPDF
    except Exception as e:
        raise RuntimeError(
            "Missing dependency for PDF processing. Install 'pymupdf' (fitz) in your environment."
        ) from e

    import re

    src_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = []
    # Capture first material code per page from text: P<digits>...
    pattern = re.compile(r"\bP(\d{2,})[A-Za-z0-9]*\b")
    for i in range(src_doc.page_count):
        text = (src_doc.load_page(i).get_text("text") or "")
        m = pattern.search(text)
        code = m.group(1) if m else None
        token = m.group(0) if m else None
        pages.append({"page_index": i, "code": code, "token": token})

    if optimized_out is None or optimized_out.empty:
        raise ValueError("Optimized output is empty; cannot reorder barcodes")
    if "orig_index" not in optimized_out.columns:
        raise ValueError("Optimized output is missing orig_index; cannot reorder barcodes")
    if "material" not in optimized_out.columns:
        raise ValueError("Optimized output is missing material; cannot reorder barcodes")

    # Reconstruct original order from orig_index
    original_order = optimized_out.sort_values(["orig_index"]).reset_index(drop=True)
    original_order["_mat_prefix"] = original_order["material"].apply(_extract_material_prefix)

    # Align pages to original rows sequentially
    row_to_page: dict[int, int] = {}
    skipped_pages: list[int] = []
    used_pages: set[int] = set()
    p = 0
    for _, row in original_order.iterrows():
        oid = int(row["orig_index"]) if pd.notna(row["orig_index"]) else None
        if oid is None:
            continue
        want = str(row.get("_mat_prefix", "") or "").strip()
        if not want:
            continue

        # Find next matching page
        while p < len(pages):
            page = pages[p]
            if page["code"] is None:
                skipped_pages.append(page["page_index"])
                p += 1
                continue
            if str(page["code"]) == want:
                row_to_page[oid] = page["page_index"]
                used_pages.add(page["page_index"])
                p += 1
                break
            # mismatch: move forward
            p += 1

    # Build reorder list in optimized order
    optimized_order = optimized_out.sort_values(["optimized_group", "orig_index"]).reset_index(drop=True)
    reordered_page_indices: list[int] = []
    missing_rows: list[dict] = []
    for _, row in optimized_order.iterrows():
        oid = int(row["orig_index"]) if pd.notna(row["orig_index"]) else None
        if oid is None:
            continue
        page_idx = row_to_page.get(oid)
        if page_idx is None:
            missing_rows.append({
                "orig_index": oid,
                "optimized_group": row.get("optimized_group"),
                "material": row.get("material"),
            })
            continue
        reordered_page_indices.append(page_idx)

    # Append any remaining pages not used (keeps information, but after reordered list)
    remaining_pages = [p["page_index"] for p in pages if p["page_index"] not in used_pages]
    final_pages = reordered_page_indices + remaining_pages

    # Create new PDF
    new_doc = fitz.open()
    for page_idx in final_pages:
        new_doc.insert_pdf(src_doc, from_page=page_idx, to_page=page_idx)

    out_buf = io.BytesIO(new_doc.tobytes())
    out_buf.seek(0)

    report = pd.DataFrame({
        "metric": [
            "pdf_pages_total",
            "pdf_pages_with_code",
            "pdf_pages_skipped_no_code",
            "rows_total",
            "rows_mapped_to_pdf",
            "rows_missing_pdf",
        ],
        "value": [
            len(pages),
            sum(1 for x in pages if x["code"] is not None),
            len(skipped_pages),
            len(original_order),
            len(row_to_page),
            len(missing_rows),
        ],
    })

    return out_buf, report

# --------- Sidebar: Offcut Controls ---------
st.sidebar.header("🧠 Offcut Settings")

# Global precut (applies to all offcuts)
precut_mm = st.sidebar.number_input("Global precut (mm)", min_value=0.0, value=0.0, step=1.0, help="Applied to every offcut; you can change this later.")

# Upload offcut inventory
st.sidebar.subheader("📤 Upload Reusable Offcuts (Optional)")
uploaded_offcuts = st.sidebar.file_uploader(
    "Upload offcut inventory CSV",
    type=["csv"],
    help="Upload a CSV with columns: material, offcut_length, precut_mm"
)

if uploaded_offcuts is not None:
    try:
        offcut_preview = pd.read_csv(uploaded_offcuts)
        uploaded_offcuts.seek(0)  # Reset file pointer
        st.sidebar.success(f"✅ {len(offcut_preview)} offcuts loaded")
        st.sidebar.dataframe(
            offcut_preview[["material", "offcut_length", "precut_mm"]],
            use_container_width=True,
            height=240
        )
        st.sidebar.caption("Scroll the table to see all loaded offcuts.")
    except Exception as e:
        st.sidebar.error(f"❌ Error reading file: {e}")

# --------- File Upload ----------
uploaded = st.file_uploader("Upload your cut batch file (.xls, .xlsx, .csv)", type=["xls", "xlsx", "csv"])

# Barcode PDF upload (for reordering to match optimized output)
barcode_pdf = st.file_uploader("Upload barcode PDF (optional)", type=["pdf"])

def optimize_one_material(subdf: pd.DataFrame, mem_pool: list[float]) -> tuple[pd.DataFrame, list[float]]:
    """
    subdf: rows of one material (with columns: orig_index, length, material)
    mem_pool: list of offcut lengths (>330) for THIS material (already precut-adjusted if needed)
    Returns:
      - assignment df for this material
      - updated mem_pool after consumption/creation
    Logic:
      - Reorder rows to minimize waste (descending length).
      - Each group uses one capacity: either the largest usable offcut (>= largest remaining item), else fresh 4880.
      - sum(length) <= capacity (no kerf).
      - Leftover > 330 saved back to mem_pool (carried forward for future batches).
    """
    # items: (orig_index, length), sort desc
    items = (
        subdf[["orig_index", "length"]]
        .sort_values("length", ascending=False)
        .values
        .tolist()
    )

    # work on a local copy of pool
    offcuts = sorted([oc for oc in mem_pool if oc > MIN_REUSE], reverse=True)
    groups = []
    mgroup = 1

    while items:
        largest_len = max(L for _, L in items)
        # choose the largest offcut that can fit the largest remaining item
        usable = [oc for oc in offcuts if oc >= largest_len]
        if usable:
            cap = usable[0]
            offcuts.remove(cap)
        else:
            cap = STOCK

        total = 0.0
        placed_idx = []
        for i, (oid, L) in enumerate(items):
            if total + L <= cap:
                placed_idx.append(i)
                total += L

        if not placed_idx:
            placed_idx = [0]
            total = items[0][1]

        leftover = cap - total
        if leftover > MIN_REUSE:
            offcuts.append(leftover)

        groups.append({
            "mat_group": mgroup,
            "capacity": cap,
            "leftover": leftover,
            "ids": [items[i][0] for i in placed_idx]
        })

        placed_set = set(placed_idx)
        items = [items[i] for i in range(len(items)) if i not in placed_set]
        mgroup += 1

    # explode to rows
    rows = []
    for g in groups:
        for oid in g["ids"]:
            rows.append({
                "orig_index": oid,
                "mat_group": g["mat_group"],
                "source_length_mm": g["capacity"],
                "group_wastage_mm": round(g["leftover"], 3),
                "carryover_offcut_mm": round(g["leftover"], 3) if g["leftover"] > MIN_REUSE else None
            })

    # return assignments + updated pool
    return pd.DataFrame(rows), offcuts

def optimize_with_memory(df_in: pd.DataFrame, offcut_mem_df: pd.DataFrame, precut_mm: float):
    """
    df_in: uploaded batch (any columns; needs at least 'length' and optional 'material')
    offcut_mem_df: global memory of offcuts across batches
    precut_mm: global adjustment applied to new offcuts saved now (not subtracted from capacity for usage yet).
               (You said keep 0 for now; we store it with each offcut for future use.)
    """
    # Prepare input
    df = df_in.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.reset_index().rename(columns={"index": "orig_index"})
    if "material" not in df.columns:
        df["material"] = "MATERIAL_1"
    df["material"] = df["material"].fillna("MATERIAL_1")
    df["length"] = pd.to_numeric(df["length"], errors="coerce")
    valid = df[df["length"].notna()].copy()

    # Build material order
    materials = list(valid["material"].astype(str).drop_duplicates())

    # Build a dict of offcut pools per material from memory
    mem_pools = {m: [] for m in materials}
    if not offcut_mem_df.empty:
        for m in materials:
            pool = offcut_mem_df.loc[offcut_mem_df["material"] == m, "offcut_length"].tolist()
            # (precut policy: we are *storing* precut, not subtracting from capacity now)
            mem_pools[m] = pool

    # Optimize per material using its pool
    parts = []
    updated_pools = {}
    for m in materials:
        sub = valid[valid["material"] == m]
        assign_m, pool_after = optimize_one_material(sub, mem_pools.get(m, []))
        assign_m["material"] = m
        parts.append(assign_m)
        updated_pools[m] = pool_after

    opt = pd.concat(parts, ignore_index=True)

    # Global group numbering: by material then material-group
    opt = opt.sort_values(["material", "mat_group"]).reset_index(drop=True)
    unique_pairs = opt[["material", "mat_group"]].drop_duplicates().reset_index(drop=True)
    pair_to_global = {(row.material, row.mat_group): i + 1 for i, row in unique_pairs.iterrows()}
    opt["optimized_group"] = opt.apply(lambda r: pair_to_global[(r["material"], r["mat_group"])], axis=1)
    opt = opt.drop(columns=["mat_group"])

    # Merge back, keep original fields intact
    out = df.merge(opt, on=["orig_index", "material"], how="left")
    out = out.sort_values(["optimized_group", "orig_index"]).reset_index(drop=True)

    # Prepare export view (hide internal/helper/noise columns)
    export_cols_to_drop = {"orig_index", "group"}
    unnamed_cols = {c for c in out.columns if str(c).strip().lower().startswith("unnamed")}
    out_export = out.drop(columns=[c for c in out.columns if c in export_cols_to_drop or c in unnamed_cols], errors="ignore")

    # Build checks
    check = (
        out.groupby("optimized_group")
        .agg(
            capacity=("source_length_mm", "first"),
            sum_lengths=("length", "sum"),
            materials=("material", lambda s: ",".join(sorted(set(str(x) for x in s if pd.notna(x))))),
        )
        .reset_index()
    )
    check["ok_capacity"] = check["sum_lengths"] <= check["capacity"] + 1e-9
    check["ok_single_material"] = ~check["materials"].str.contains(",")

    # --- Update offcut memory for NEXT batches ---
    # 1) Start from remaining pools (unused offcuts from previous memory)
    new_mem_records = []
    for m, pool in updated_pools.items():
        for oc in pool:
            new_mem_records.append({
                "material": m,
                "offcut_length": oc,
                "precut_mm": precut_mm,
                "batch_id": datetime.now().strftime("%Y%m%d-%H%M%S"),
                "timestamp": datetime.now().isoformat(timespec="seconds"),
            })

    # 2) Create DataFrame (no longer saving to file for cloud hosting)
    new_mem_df = pd.DataFrame(new_mem_records, columns=["material", "offcut_length", "precut_mm", "batch_id", "timestamp"])

    # 3) Create downloadable buffers
    excel_buf = io.BytesIO()
    with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
        out_export.to_excel(writer, sheet_name="Optimized", index=False)
        check.to_excel(writer, sheet_name="Checks", index=False)
    excel_buf.seek(0)

    return out, check, excel_buf, new_mem_df

# ---------- Run when file uploaded ----------
if uploaded is not None:
    st.success("✅ File uploaded. Click to generate optimized output using saved offcuts first.")
    if st.button("Generate optimized files"):
        try:
            if uploaded.name.lower().endswith(".csv"):
                df_input = pd.read_csv(uploaded, sep=None, engine="python")  # auto-detect delimiter
            else:
                df_input = pd.read_excel(uploaded, engine="openpyxl")
        except Exception as e:
            st.error(f"❌ Could not read the file: {e}")
            st.stop()
        
        # Use uploaded offcuts if provided, otherwise optimize without offcuts
        if uploaded_offcuts is not None:
            try:
                offcut_to_use = pd.read_csv(uploaded_offcuts)
                st.info(f"ℹ️ Using uploaded offcut inventory ({len(offcut_to_use)} offcuts)")
            except Exception as e:
                st.warning(f"⚠️ Could not read uploaded offcut file: {e}. Optimizing without offcuts.")
                offcut_to_use = pd.DataFrame(columns=["material", "offcut_length", "precut_mm", "batch_id", "timestamp"])
        else:
            offcut_to_use = pd.DataFrame(columns=["material", "offcut_length", "precut_mm", "batch_id", "timestamp"])
            st.info("ℹ️ No offcut inventory uploaded. Optimizing with fresh stock only.")

        out, check, excel_buf, new_mem_df = optimize_with_memory(df_input, offcut_to_use, precut_mm)

        # Create machine-readable file from OPTIMIZED output
        try:
            machine_df = transform_optimized_to_machine_readable(out)
            machine_buf = io.BytesIO()
            machine_df.to_csv(machine_buf, index=False, header=False, encoding="utf-8")
            machine_buf.seek(0)
        except Exception as e:
            machine_buf = None
            st.warning(f"⚠️ Could not generate machine readable file: {e}")

        # Reorder barcode PDF (if provided) to match optimized sequence
        barcode_buf = None
        barcode_report = None
        if barcode_pdf is not None:
            try:
                pdf_bytes = barcode_pdf.getvalue()
                barcode_buf, barcode_report = reorder_barcode_pdf_to_optimized(pdf_bytes, out)
            except Exception as e:
                barcode_buf = None
                barcode_report = None
                st.warning(f"⚠️ Could not reorder barcode PDF: {e}")

        # Store results in session state
        st.session_state.results = {
            'excel_buf': excel_buf,
            'new_mem_df': new_mem_df,
            'machine_buf': machine_buf,
            'barcode_buf': barcode_buf,
            'barcode_report': barcode_report,
        }
    
    # Display downloads if results exist in session state
    if 'results' in st.session_state:
        st.subheader("📂 Downloads")
        st.download_button(
            label="⬇️ Optimized Excel (.xlsx)",
            data=st.session_state.results['excel_buf'],
            file_name="optimized_cut_batch.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        if st.session_state.results.get('machine_buf') is not None:
            st.download_button(
                label="⬇️ Main saw machine readable format",
                data=st.session_state.results['machine_buf'],
                file_name="machine_readable_profile.csv",
                mime="text/csv",
            )

        if st.session_state.results.get('barcode_buf') is not None:
            st.download_button(
                label="⬇️ Reordered barcode PDF",
                data=st.session_state.results['barcode_buf'],
                file_name="barcodes_reordered.pdf",
                mime="application/pdf",
            )
            if st.session_state.results.get('barcode_report') is not None:
                st.caption("Barcode reorder summary")
                st.dataframe(st.session_state.results['barcode_report'], use_container_width=True)
        
        # Download button for generated offcuts
        if not st.session_state.results['new_mem_df'].empty:
            offcut_download = st.session_state.results['new_mem_df'][["material", "offcut_length", "precut_mm"]].to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ Download Generated Offcuts Inventory",
                data=offcut_download,
                file_name="reusable_offcuts_inventory.csv",
                mime="text/csv",
                help="Save this file and upload it next time to reuse these offcuts"
            )

        st.subheader("🧧 Generated Reusable Offcuts")
        if not st.session_state.results['new_mem_df'].empty:
            st.dataframe(
                st.session_state.results['new_mem_df'][["material", "offcut_length"]].sort_values(["material", "offcut_length"], ascending=[True, False]),
                use_container_width=True
            )
            st.caption(f"💾 Save the offcut inventory above and upload it next time to reuse {len(st.session_state.results['new_mem_df'])} offcuts")
        else:
            st.info("No reusable offcuts generated (all cuts used full stock or waste < 330mm)")
else:
    st.info("Upload an Excel or CSV file to begin.")
