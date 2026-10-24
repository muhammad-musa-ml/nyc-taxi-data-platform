"""Turn the raw files in results/ into results/summary.json and results/SUMMARY.md.

    python scripts/experiments/summarize.py

Every number in the README is copied from SUMMARY.md. Medians are over the runs present; the range is min-max.
Nothing is estimated: a quantity that no file holds is printed as "not measured".
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

from common import RESULTS, write_json

MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]
TASKS = ["load_zones", "fetch_to_landing", "validate_with_great_expectations", "load_warehouse", "dbt_run",
         "dbt_test", "reconcile_and_check_freshness"]


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def med(values: list[float]) -> dict:
    values = [v for v in values if v is not None]
    if not values:
        return {"n": 0}
    return {"n": len(values), "median": round(statistics.median(values), 1), "min": round(min(values), 1),
            "max": round(max(values), 1)}


def fmt(m: dict, unit: str = "s") -> str:
    if not m.get("n"):
        return "not measured"
    return f"{m['median']:g} {unit} ({m['min']:g}-{m['max']:g}, n={m['n']})"


def backfill_section(out: dict, lines: list[str]) -> None:
    runs = [load(p) for p in sorted((RESULTS / "backfill").glob("run-*/result.json"))]
    runs = [r for r in runs if r]
    if not runs:
        lines += ["## Backfill", "", "not measured", ""]
        return
    ok = [r for r in runs if r["exit_code"] == 0]
    out["backfill"] = {"runs": len(runs), "runs_succeeded": len(ok), "wall_seconds": med([r["wall_seconds"] for r in ok])}
    per_month = {}
    per_task = {}
    for month in MONTHS:
        per_month[month] = med([r["dag_runs"][month]["dag_run_seconds"] for r in ok if month in r["dag_runs"]])
        for task in TASKS:
            per_task.setdefault(task, {})[month] = med(
                [r["dag_runs"][month]["tasks"][task]["seconds"] for r in ok if month in r["dag_runs"]
                 and task in r["dag_runs"][month]["tasks"]])
    out["backfill"]["dag_run_seconds_per_month"] = per_month
    out["backfill"]["task_seconds_per_month"] = per_task
    downloads = {}
    for month in MONTHS:
        vals = [t["seconds"] for r in ok for t in r["stage_timings"]
                if t["stage"] == "fetch" and t["month"] == month + "-01" and not t["detail"].get("cached")]
        downloads[month] = med(vals)
    out["backfill"]["download_seconds_per_month"] = downloads
    sizes = {m: ok[0]["state"]["ledger"][m + "-01"]["file_bytes"] for m in MONTHS} if ok else {}
    out["backfill"]["file_bytes"] = sizes
    lines += ["## Backfill from an empty stack (all six months)", "",
              f"{len(ok)} of {len(runs)} runs succeeded. Each run deleted the Compose volumes first and downloaded all six files "
              "from the TLC site into an empty landing bucket.", "",
              f"- wall time of `airflow dags backfill` for 2025-01 .. 2025-06: **{fmt(out['backfill']['wall_seconds'])}**",
              "- per run: " + ", ".join(f"{r['wall_seconds']:.1f} s" for r in ok), "",
              "| month | DAG run | " + " | ".join(t.replace("_", " ") for t in TASKS) + " |",
              "|---|---|" + "---|" * len(TASKS)]
    for month in MONTHS:
        cells = [fmt(per_task[t][month]).split(" (")[0] for t in TASKS]
        lines.append(f"| {month} | {fmt(per_month[month]).split(' (')[0]} | " + " | ".join(cells) + " |")
    lines += ["", "Median seconds over the runs; ranges are in `summary.json`. The fetch task includes the download.", ""]
    lines += ["Download of each file from the TLC site (seconds, fetch task):", ""]
    lines += [f"- {m}: {fmt(downloads[m])}, {sizes.get(m, 0):,} bytes" for m in MONTHS] + [""]

    # counts must be identical in every run
    first = ok[0]["state"]["ledger"]
    identical = all(r["state"]["ledger"] == first and r["state"]["checks"] == ok[0]["state"]["checks"] for r in ok)
    out["backfill"]["counts_identical_across_runs"] = identical
    out["backfill"]["counts"] = {"ledger": first, "checks": ok[0]["state"]["checks"]}
    lines += ["## Rows in, loaded and quarantined per month", "",
              f"Identical in all {len(ok)} runs: {'yes' if identical else 'NO'}", "",
              "| month | rows in | loaded | quarantined | share quarantined |", "|---|---:|---:|---:|---:|"]
    tot = [0, 0, 0]
    for m in MONTHS:
        row = first[m + "-01"]
        tot = [tot[0] + row["rows_in"], tot[1] + row["rows_loaded"], tot[2] + row["rows_quarantined"]]
        lines.append(f"| {m} | {row['rows_in']:,} | {row['rows_loaded']:,} | {row['rows_quarantined']:,} | "
                     f"{100 * row['rows_quarantined'] / row['rows_in']:.2f}% |")
    lines.append(f"| total | {tot[0]:,} | {tot[1]:,} | {tot[2]:,} | {100 * tot[2] / tot[0]:.2f}% |")
    out["backfill"]["totals"] = {"rows_in": tot[0], "rows_loaded": tot[1], "rows_quarantined": tot[2]}
    codes = sorted({c for m in ok[0]["state"]["checks"].values() for c in m})
    lines += ["", "Rows that failed each rule (a row can fail several; the first failing rule is its primary reason):", "",
              "| reason code | " + " | ".join(MONTHS) + " |", "|---|" + "---:|" * len(MONTHS)]
    for code in codes:
        cells = [f"{ok[0]['state']['checks'].get(m + '-01', {}).get(code, {}).get('rows_failed', 0):,}" for m in MONTHS]
        lines.append(f"| {code} | " + " | ".join(cells) + " |")
    lines.append("")

    replays = [(p, load(p)) for p in sorted((RESULTS / "backfill").glob("run-*/replay.json"))]
    replays = [(p, r) for p, r in replays if r]
    if replays:
        out["replay"] = [{"run": p.parent.name, "wall_seconds": r["wall_seconds"], "exit_code": r["exit_code"],
                          "identical": r["comparison_with_state_before_replay"]["identical"],
                          "deltas": r["comparison_with_state_before_replay"]["deltas"],
                          "load_count_per_month": r["load_count_per_month"]} for p, r in replays]
        lines += ["## Replay of the whole range", ""]
        for item in out["replay"]:
            lines.append(f"- {item['run']}: {item['wall_seconds']:.1f} s, exit {item['exit_code']}, state identical to the state "
                         f"before the replay: {'yes' if item['identical'] else 'NO'}, load count per month "
                         f"{sorted(set(item['load_count_per_month'].values()))}")
        lines.append("")


def idempotency_section(out: dict, lines: list[str]) -> None:
    data = load(RESULTS / "idempotency" / "result.json")
    if not data:
        lines += ["## Idempotency", "", "not measured", ""]
        return
    out["idempotency"] = {k: data[k] for k in ("reruns_per_month", "total_reruns", "reruns_with_any_difference_or_failure",
                                                "reruns_with_duplicate_keys")}
    lines += ["## Idempotency (re-running a loaded month)", "",
              f"{data['total_reruns']} re-runs ({data['reruns_per_month']} per month, six months) of `taxi-platform run-month`.",
              f"- re-runs after which any row count, content hash or ledger count differed from the state before the first "
              f"re-run, or that failed: **{data['reruns_with_any_difference_or_failure']}**",
              f"- re-runs after which any primary key appeared twice: **{data['reruns_with_duplicate_keys']}**", ""]


def crash_section(out: dict, lines: list[str]) -> None:
    data = load(RESULTS / "crash" / "result.json")
    if not data:
        lines += ["## Loader killed mid-transaction", "", "not measured", ""]
        return
    ok = [t for t in data["trials"] if "error" not in t]
    out["crash"] = {"month": data["month"], "trials": len(data["trials"]), "valid_trials": len(ok),
                    "killed_mid_copy": data["trials_killed_mid_copy"],
                    "unchanged_after_kill": data["trials_left_unchanged_by_the_kill"],
                    "identical_after_retry": data["trials_identical_after_retry"],
                    "seconds_until_released": med([t["seconds_until_postgres_released_the_dead_loader"] for t in ok])}
    lines += ["## Loader killed in the middle of its transaction", "",
              f"{len(data['trials'])} trials on {data['month']} (SIGKILL to the loader's container after 0-24 s of its COPY).",
              f"- killed while the COPY was still running: {data['trials_killed_mid_copy']}",
              f"- warehouse identical to before the kill: {data['trials_left_unchanged_by_the_kill']} of {len(ok)}",
              f"- warehouse identical to before after running the load again: {data['trials_identical_after_retry']} of {len(ok)}",
              f"- seconds until PostgreSQL released the dead loader's locks: {fmt(out['crash']['seconds_until_released'])}", ""]


def injection_section(out: dict, lines: list[str]) -> None:
    seeds = [load(p) for p in sorted((RESULTS / "injection").glob("seed-*.json"))]
    seeds = [s for s in seeds if s and "score" in s]
    if not seeds:
        lines += ["## Injected bad rows", "", "not measured", ""]
        return
    names = list(seeds[0]["score"]["per_fault"])
    per_fault = {}
    for name in names:
        slots = [s["score"]["per_fault"][name] for s in seeds]
        per_fault[name] = {"expected_reason_code": slots[0]["expected_reason_code"], "detectable": slots[0]["detectable"],
                           "injected": med([x["injected"] for x in slots]),
                           "caught": med([x["caught_by_expected_code"] for x in slots]),
                           "other_code_only": med([x["quarantined_by_other_code_only"] for x in slots]),
                           "loaded": med([x["loaded"] for x in slots])}
    det = {k: med([s["score"]["detectable_total"][k] for s in seeds]) for k in
           ("injected", "caught_by_expected_code", "quarantined_by_other_code_only", "loaded")}
    und = {k: med([s["score"]["undetectable_total"][k] for s in seeds]) for k in
           ("injected", "caught_by_expected_code", "quarantined_by_other_code_only", "loaded")}
    extra = med([s["score"]["unexpected_extra_quarantined_rows"] for s in seeds])
    out["injection"] = {"seeds": len(seeds), "month": seeds[0]["month"], "per_fault_count": seeds[0]["per_fault"],
                        "detectable_total": det, "undetectable_total": und, "unexpected_extra_quarantined": extra,
                        "per_fault": per_fault, "agrees_with_loaded_table": all(s["loaded_count_agrees_with_score"] for s in seeds)}
    lines += ["## Injected bad rows", "",
              f"{len(seeds)} runs (seeds), each injecting {seeds[0]['per_fault']} rows per fault into real rows of "
              f"{seeds[0]['month']} that had passed every rule, then running the whole pipeline on the result. "
              f"Medians over the runs, range in brackets.", "",
              f"- detectable faults: injected {fmt(det['injected'], 'rows')}, caught by the expected reason code "
              f"{fmt(det['caught_by_expected_code'], 'rows')}, quarantined only under another code "
              f"{fmt(det['quarantined_by_other_code_only'], 'rows')}, loaded (missed) {fmt(det['loaded'], 'rows')}",
              f"- undetectable faults (plausible values): injected {fmt(und['injected'], 'rows')}, loaded "
              f"{fmt(und['loaded'], 'rows')}",
              f"- rows the pipeline rejected beyond the original file's own rejects and the injected rows: {fmt(extra, 'rows')}",
              f"- injected rows found in the loaded table agree with the score in every run: "
              f"{'yes' if out['injection']['agrees_with_loaded_table'] else 'NO'}", "",
              "| fault | expected reason code | injected | caught by that code | other code only | loaded (missed) |",
              "|---|---|---:|---:|---:|---:|"]
    for name, v in per_fault.items():
        lines.append(f"| {name} | {v['expected_reason_code'] or 'none: not detectable'} | {v['injected']['median']:g} | "
                     f"{v['caught']['median']:g} | {v['other_code_only']['median']:g} | {v['loaded']['median']:g} |")
    lines.append("")


def alerts_section(out: dict, lines: list[str]) -> None:
    contract = load(RESULTS / "alerts" / "contract-faults.json")
    fresh = load(RESULTS / "alerts" / "freshness.json")
    lines += ["## Alerts", ""]
    if contract:
        per = {}
        for fault in contract["fault_to_dag_month"]:
            rows = [r["faults"][fault] for r in contract["repeats"]]
            per[fault] = {
                "failed_without_retry": all(x["validate_state"] == "failed" and x["validate_tries"] <= 1 for x in rows),
                "downstream_not_run": all(set(x["downstream_states"].values()) == {"upstream_failed"} for x in rows),
                "task_failure_records": med([x["task_failure_records"] for x in rows]),
                "dag_failure_records": med([x["dag_failure_records"] for x in rows]),
                "alert_delay_after_task_ended_s": med([x.get("alert_delay_after_task_ended_s") for x in rows]),
                "detected_after_task_started_s": med([x.get("detected_after_task_started_s") for x in rows]),
                "error": rows[0].get("error", ""),
            }
        nothing_loaded = all(r["loaded_into_warehouse"] == {"ledger_rows": 0, "raw_rows": 0} for r in contract["repeats"])
        out["contract_alerts"] = {"repeats": len(contract["repeats"]), "per_fault": per, "nothing_loaded": nothing_loaded}
        lines += [f"File-level faults offered to the monthly DAG ({len(contract['repeats'])} repeats each). Nothing loaded "
                  f"into the warehouse in any repeat: {'yes' if nothing_loaded else 'NO'}.", "",
                  "| fault | validate task | later tasks | task-failure records | DAG-failure records | alert delay after the task ended | detected after the task started |",
                  "|---|---|---|---:|---:|---|---|"]
        for fault, v in per.items():
            lines.append(f"| {fault} | {'failed, no retry' if v['failed_without_retry'] else 'CHECK'} | "
                         f"{'not run (upstream_failed)' if v['downstream_not_run'] else 'CHECK'} | "
                         f"{v['task_failure_records']['median']:g} | {v['dag_failure_records']['median']:g} | "
                         f"{fmt(v['alert_delay_after_task_ended_s'])} | {fmt(v['detected_after_task_started_s'])} |")
        lines.append("")
    else:
        lines += ["File-level faults: not measured", ""]
    if fresh:
        out["freshness_alerts"] = []
        lines += ["Freshness DAG (`taxi_freshness`), six months loaded:", ""]
        for sc in fresh["scenarios"]:
            runs = sc["runs"]
            item = {"scenario": sc["label"], "environment": sc["environment"], "repeats": len(runs),
                    "task_states": sorted({r["task_state"] for r in runs}),
                    "findings": sorted({tuple(r["findings"]) for r in runs}),
                    "first_finding_after_task_started_s": med([r.get("first_finding_after_task_started_s") for r in runs]),
                    "failure_alert_delay_after_task_ended_s": med([r.get("failure_alert_delay_after_task_ended_s") for r in runs])}
            out["freshness_alerts"].append(item)
            lines.append(f"- {sc['label']}: task {item['task_states']}, findings {[list(f) for f in item['findings']]}, first finding "
                         f"{fmt(item['first_finding_after_task_started_s'])} after the task started, failure alert "
                         f"{fmt(item['failure_alert_delay_after_task_ended_s'])} after the task ended")
        lines.append("")
    else:
        lines += ["Freshness: not measured", ""]


def main() -> None:
    out: dict = {}
    lines = ["# Results", "",
             "Generated by `scripts/experiments/summarize.py` from the raw files beside it. Medians, with min-max and the "
             "number of runs in brackets.", ""]
    backfill_section(out, lines)
    idempotency_section(out, lines)
    crash_section(out, lines)
    injection_section(out, lines)
    alerts_section(out, lines)
    write_json(RESULTS / "summary.json", out)
    (RESULTS / "SUMMARY.md").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print("wrote", RESULTS / "SUMMARY.md")


if __name__ == "__main__":
    main()
