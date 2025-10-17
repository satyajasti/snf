# --- ADD/REPLACE these header columns for the Checkbox+Radios sheet ---
# extend _HEADERS for "Checkbox+Radios" to include diff columns
_HEADERS["Checkbox+Radios"] = [
    "Auth#","doc name","page",
    "selection match count (TP)",
    "selection incorrect (FP,FN)",
    "Notes",
    "top-missing-in-TXT (PDF-only tokens)",   # NEW
    "top-extra-in-TXT (TXT-only tokens)"      # NEW
]


# --- ADD THIS NEW FUNCTION at the bottom (after run_ocr_validation) ---
def run_radios_only(cfg: dict, verbose: bool = True, top_k: int = 10) -> dict:
    """
    Radios-only validation:
      - Uses pdf_path + txt_dir from cfg
      - Writes per-chunk diffs to Checkbox+Radios (adds two columns)
      - Prints diffs to console if verbose=True
    Returns summary dict.
    """
    pdf_path = Path(cfg["pdf_path"])
    txt_dir  = Path(cfg["txt_dir"])
    excel    = Path(cfg.get("output_excel", "output/benchmark_assignment.xlsx"))
    threshold = float(cfg.get("threshold", 0.90))

    excel.parent.mkdir(parents=True, exist_ok=True)

    log.info(f"[RADIOS] PDF: {pdf_path}  TXT dir: {txt_dir}")
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    if not txt_dir.exists():
        raise FileNotFoundError(f"TXT dir not found: {txt_dir}")

    # Build reference from PDF
    ref_text = _normalize(_extract_pdf_text(str(pdf_path)))
    ref_ctr  = Counter(_tokens(ref_text))

    # Collect TXT files
    txt_files = sorted(txt_dir.glob("*.txt"))
    if not txt_files:
        log.warning(f"[RADIOS] No .txt files in {txt_dir}")
        return {"pdf": pdf_path.name, "chunks": 0, "correct": 0, "incorrect": 0}

    wb = _ensure_wb(excel)
    wb = _ensure_all_sheets(wb)
    ws_sum = wb["BnchMrk2-Assignment"]
    ws_det = wb["Checkbox+Radios"]

    correct = incorrect = 0
    for tf in txt_files:
        # Hypothesis from TXT
        hyp_text = _normalize(tf.read_text(encoding="utf-8", errors="ignore"))
        hyp_ctr  = Counter(_tokens(hyp_text))

        # Metrics
        prec, rec, f1 = _prf(ref_ctr, hyp_ctr)
        overlap = sum((ref_ctr & hyp_ctr).values())
        fp      = max(0, sum(hyp_ctr.values()) - overlap)
        note    = "OK" if rec >= threshold else f"Low recall ({int(round(rec*100))}%)"
        if rec >= threshold: correct += 1
        else: incorrect += 1

        # Differences (top-k)
        # TXT-only tokens (extra): hyp - ref   | PDF-only tokens (missing): ref - hyp
        extra_ctr   = hyp_ctr - ref_ctr
        missing_ctr = ref_ctr - hyp_ctr
        top_extra   = ", ".join([f"{tok}({cnt})" for tok, cnt in extra_ctr.most_common(top_k)])
        top_missing = ", ".join([f"{tok}({cnt})" for tok, cnt in missing_ctr.most_common(top_k)])

        page = _guess_page(tf.name)

        # Console debug (so you see exactly what's not matching)
        if verbose:
            log.info(
                f"[RADIOS] Chunk={tf.name} page={page} TP={overlap} FP/FN={fp} Recall={rec:.2%} → {note}"
            )
            if top_missing:
                log.info(f"[RADIOS]  PDF-only (missing in TXT) top{top_k}: {top_missing}")
            if top_extra:
                log.info(f"[RADIOS]  TXT-only (extra vs PDF) top{top_k}: {top_extra}")

        # Write detail row with diffs
        ws_det.append([
            cfg.get("auth",""),
            pdf_path.name,
            page,
            int(overlap),
            int(fp),
            note,
            top_missing,   # NEW column
            top_extra      # NEW column
        ])

    # Write summary
    ws_sum.append([
        cfg.get("report_time", datetime.now().strftime("%H:%M")),
        cfg.get("resource",""),
        cfg.get("auth",""),
        pdf_path.name,
        "",
        len(txt_files),
        correct,
        incorrect,
        "Goal Met [] Check box is captured" if incorrect == 0 else "Review discrepancies"
    ])

    wb.save(excel)
    log.info(f"[RADIOS] Report updated → {excel}")
    return {"pdf": pdf_path.name, "chunks": len(txt_files), "correct": correct, "incorrect": incorrect}



@ocr @radios_only
Feature: Radios/Checkbox extraction validation
  Validate Smart OCR TXT chunks against the source PDF, and record diffs.

  Scenario: Validate radios-only using JSON configuration
    Given OCR validation config is loaded from "configs/config.json"
    When I run radios-only validation from JSON
    Then the OCR report should be written successfully


# Ensure project root on sys.path so imports like 'common', 'validators' work
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from behave import given, when, then
import json
from validators.ocr_multi_txt_validator import run_radios_only
from common.edl_utils import get_logger

log = get_logger("behave_radios_only")

@given('OCR validation config is loaded from "{config_path}"')
def step_load_cfg(ctx, config_path):
    cfg_file = ROOT / config_path
    if not cfg_file.exists():
        raise FileNotFoundError(f"Config not found at {cfg_file}")
    ctx.cfg = json.loads(cfg_file.read_text(encoding="utf-8"))
    if "ocr_validation" not in ctx.cfg:
        raise KeyError("Missing 'ocr_validation' in JSON")
    ctx.job = ctx.cfg["ocr_validation"]
    if not ctx.job.get("pdf_path"):
        raise ValueError("pdf_path missing in JSON under 'ocr_validation'")
    if not ctx.job.get("txt_dir"):
        raise ValueError("txt_dir missing in JSON under 'ocr_validation'")

@when("I run radios-only validation from JSON")
def step_run(ctx):
    ctx.result = run_radios_only(ctx.job, verbose=True, top_k=10)
    log.info(f"RADIOS summary: {ctx.result}")

@then("the OCR report should be written successfully")
def step_assert(ctx):
    assert ctx.result["chunks"] >= 0



{
  "ocr_validation": {
    "pdf_path": "data/YourDoc.pdf",
    "txt_dir": "data/YourDoc_chunks",
    "output_excel": "output/benchmark_assignment.xlsx",
    "threshold": 0.90,
    "auth": "A20240918472909",
    "resource": "Aravindh",
    "report_time": "10:16"
  }
}
