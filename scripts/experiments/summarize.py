"""Turn the raw files in results/ into results/summary.json and results/SUMMARY.md.

    python scripts/experiments/summarize.py

Every number in the README is copied from SUMMARY.md. Medians are over the runs present; the range is min-max.
Nothing is estimated: a quantity that no file holds is printed as "not measured".
"""
from __future__ import annotations

import json
import re
import statistics
from datetime import datetime
from pathlib import Path

from common import RESULTS, write_json

MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]
TASKS = ["load_zones", "fetch_to_landing", "validate_with_great_expectations", "load_warehouse", "dbt_run",
         "dbt_test", "reconcile_and_check_freshness"]


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def med(values: list[float], digits: int = 1) -> dict:
    values = [v for v in values if v is not None]
    if not values:
        return {"n": 0}
    return {"n": len(values), "median": round(statistics.median(values), digits), "min": round(min(values), digits),
            "max": round(max(values), digits)}


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
    heads = {}
    for path in sorted((RESULTS / "backfill").glob("run-*/environment.json")):
        code = (load(path) or {}).get("code", {})
        heads[path.parent.name] = {"git_head": code.get("git_head", "not recorded"),
                                   "uncommitted": code.get("uncommitted_changes_to_pipeline_dags_dbt_or_compose")}
    out["backfill"]["code_per_run"] = heads
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
    # The zone lookup is 12 KB: its stage takes well under a second unless the download had to wait and retry
    # (the first back-off is 2 s) or fall back to the landed copy.
    zone_stages = [t for r in ok for t in r["stage_timings"] if t["stage"] == "load_zones"]
    out["backfill"]["load_zones_stage"] = {
        "stage_records": len(zone_stages), "max_seconds": round(max((t["seconds"] for t in zone_stages), default=0), 2),
        "records_from_the_landed_copy": sum(1 for t in zone_stages if t["detail"].get("source") == "landed copy")}
    lines += ["## Backfill from an empty stack (all six months)", "",
              f"{len(ok)} of {len(runs)} runs succeeded. Each run deleted the Compose volumes first and downloaded all six files "
              "from the TLC site into an empty landing bucket.", "",
              f"- wall time of `airflow dags backfill` for 2025-01 .. 2025-06: **{fmt(out['backfill']['wall_seconds'])}**",
              "- per run: " + ", ".join(f"{r['wall_seconds']:.1f} s" for r in ok),
              "- code revision per run: " + "; ".join(
                  f"{run} not recorded" if head["git_head"] == "not recorded" else
                  f"{run} {head['git_head'][:10]}" + (", with uncommitted changes" if head["uncommitted"] else ", clean")
                  for run, head in heads.items()), "",
              "| month | DAG run | " + " | ".join(t.replace("_", " ") for t in TASKS) + " |",
              "|---|---|" + "---|" * len(TASKS)]
    for month in MONTHS:
        cells = [fmt(per_task[t][month]).split(" (")[0] for t in TASKS]
        lines.append(f"| {month} | {fmt(per_month[month]).split(' (')[0]} | " + " | ".join(cells) + " |")
    lines += ["", "Median seconds over the runs; ranges are in `summary.json`. The fetch task includes the download.", ""]
    zs = out["backfill"]["load_zones_stage"]
    lines += [f"The `load_zones` stage ran {zs['stage_records']} times across these runs; the longest took {zs['max_seconds']:g} s "
              f"(a download that had to retry would have waited at least 2 s), and {zs['records_from_the_landed_copy']} used the landed copy.", ""]
    lines += ["Download of each file from the TLC site (seconds, fetch task):", ""]
    lines += [f"- {m}: {fmt(downloads[m])}, {sizes.get(m, 0):,} bytes" for m in MONTHS] + [""]

    # the pipeline's downloads must be the files an independent download produced
    independent = load(RESULTS / "source-files.json")
    if independent:
        same = [all(r["state"]["ledger"][m + "-01"]["file_sha256"] == independent["files"][m]["sha256"]
                    and r["state"]["ledger"][m + "-01"]["file_bytes"] == independent["files"][m]["bytes"] for m in MONTHS)
                for r in ok]
        zone_digests = {t["detail"].get("sha256") for r in ok for t in r["stage_timings"]
                        if t["stage"] == "load_zones" and t["detail"].get("sha256")}
        out["backfill"]["downloads_match_independent_digests"] = {
            "runs_matching": sum(same), "runs": len(ok), "zone_lookup_match": zone_digests == {independent["zone_lookup"]["sha256"]}}
        lines += [f"The six files the pipeline downloaded had the same SHA-256 and size as an independent `curl` download "
                  f"(`source-files.json`, fetched {independent['fetched_at_utc']}) in {sum(same)} of {len(ok)} runs; the zone lookup "
                  f"digest matched: {'yes' if zone_digests == {independent['zone_lookup']['sha256']} else 'NO'}.", ""]

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
                    "finished_before_the_kill": data["trials_finished_before_the_kill"],
                    "unchanged_after_kill": data["trials_left_unchanged_by_the_kill"],
                    "identical_after_retry": data["trials_identical_after_retry"],
                    "seconds_until_released": med([t["seconds_until_postgres_released_the_dead_loader"] for t in ok])}
    delays = [t["extra_delay_s"] for t in ok]
    lines += ["## Loader killed in the middle of its transaction", "",
              f"{len(data['trials'])} trials on {data['month']}: SIGKILL to the loader's container {min(delays):g} to "
              f"{max(delays):g} s after PostgreSQL first showed its COPY running.",
              f"- killed while the COPY was still running: {data['trials_killed_mid_copy']} "
              f"(already finished before the kill: {data['trials_finished_before_the_kill']})",
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
                "failed_without_retry": all(x["validate_state"] == "failed" and x["validate_failed_attempts"] == 1 for x in rows),
                "downstream_not_run": all(set(x["downstream_states"].values()) == {"upstream_failed"} for x in rows),
                "task_failure_records": med([x["task_failure_records"] for x in rows]),
                "dag_failure_records": med([x["dag_failure_records"] for x in rows]),
                "alert_delay_after_task_ended_s": med([x.get("alert_delay_after_task_ended_s") for x in rows], 4),
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
                    "failure_alert_delay_after_task_ended_s": med([r.get("failure_alert_delay_after_task_ended_s") for r in runs], 4)}
            out["freshness_alerts"].append(item)
            lines.append(f"- {sc['label']}: task {item['task_states']}, findings {[list(f) for f in item['findings']]}, first finding "
                         f"{fmt(item['first_finding_after_task_started_s'])} after the task started, failure alert "
                         f"{fmt(item['failure_alert_delay_after_task_ended_s'])} after the task ended")
        lines.append("")
    else:
        lines += ["Freshness: not measured", ""]


def earlier_attempts_section(out: dict, lines: list[str]) -> None:
    """The first tries of four experiments went wrong in ways worth keeping; their files stay in results/."""
    e: dict = {}
    lines += ["## Earlier attempts that found problems (files kept)", ""]

    replay = load(RESULTS / "before-mart-fix" / "backfill-run-1" / "replay.json")
    if replay:
        deltas = replay["comparison_with_state_before_replay"]["deltas"]
        e["replay_before_the_mart_fix"] = {"identical": replay["comparison_with_state_before_replay"]["identical"],
                                           "differences": len(deltas),
                                           "tables": sorted({d["table"] for d in deltas}),
                                           "months": len({d["month"] for d in deltas}),
                                           "row_count_changes": sum(1 for d in deltas if d.get("row_delta"))}
        r = e["replay_before_the_mart_fix"]
        lines += [f"- Replay of all six months before the aggregate marts were made exact (`before-mart-fix/`): identical: "
                  f"{'yes' if r['identical'] else 'NO'}; {r['differences']} table-and-month differences in {', '.join(r['tables'])} "
                  f"across {r['months']} months, {r['row_count_changes']} of them in a row count."]

    first = load(RESULTS / "idempotency-first-attempt" / "result.json")
    if first:
        bad = [x for x in first["runs"] if x["exit_code"] != 0]
        e["idempotency_first_attempt"] = {
            "reruns": first["total_reruns"], "failed": len(bad),
            "failed_reruns": [f"{x['month']} re-run {x['rerun']}" for x in bad],
            "state_unchanged_after_every_failure": all(x["identical_to_state_before_the_first_rerun"] for x in bad),
            "duplicate_keys": first["reruns_with_duplicate_keys"]}
        f = e["idempotency_first_attempt"]
        lines += [f"- First idempotency attempt (`idempotency-first-attempt/`): {f['reruns']} re-runs, {f['failed']} exited with "
                  f"status 1 ({', '.join(f['failed_reruns'])}); the warehouse was identical to before after each of them: "
                  f"{'yes' if f['state_unchanged_after_every_failure'] else 'NO'}; duplicate keys: {f['duplicate_keys']}."]

    episode = RESULTS / "zone-download-probe" / "202-episode" / "output.txt"
    if episode.exists():
        text = episode.read_text(encoding="utf-8")
        tally = {int(m.group(2)): int(m.group(1)) for m in re.finditer(r"(?m)^(\d+) (\d{3}) bytes=\d+", text)}
        indexes = [int(m.group(1)) for m in re.finditer(r"(?m)^(\d+): 202 ", text)]
        e["zone_requests_202"] = {"requests": sum(tally.values()), "answered_202_empty": tally.get(202, 0),
                                  "answered_200": tally.get(200, 0),
                                  "202s_were_the_first_in_a_row": bool(indexes) and min(indexes) == 0 and max(indexes) + 1 == len(indexes)}
        z = e["zone_requests_202"]
        lines += [f"- Plain requests for the zone lookup (`zone-download-probe/202-episode/`): {z['requests']} requests, "
                  f"{z['answered_202_empty']} answered HTTP 202 with an empty body"
                  f"{' (the first ' + str(z['answered_202_empty']) + ' in a row)' if z['202s_were_the_first_in_a_row'] else ''}, "
                  f"{z['answered_200']} answered 200."]

    for name, label in (("run-1-with-the-202-fix-only", "pipeline download with only the 202 fix"),
                        ("run-2-final-code", "pipeline download, final code")):
        probe = load(RESULTS / "zone-download-probe" / name / "result.json")
        if not probe:
            continue
        b = probe["part_b_pipeline_download"]
        answers = sorted({a for c in b["detail"] if c["error"] for a in c["http_answers"]})
        failed = [c for c in b["detail"] if c["error"]]
        span = ((datetime.fromisoformat(failed[-1]["at"]) - datetime.fromisoformat(failed[0]["at"])).total_seconds()
                if failed else None)
        e[name] = {"calls": b["calls"], "right_file": b["calls_that_ended_with_the_right_file"], "failed": b["calls_that_failed"],
                   "needed_a_retry": b["calls_that_needed_a_retry"], "failed_http_answers": answers,
                   "failed_calls_span_s": None if span is None else round(span, 1),
                   "plain_requests": probe["part_a_plain_requests"]["requests"],
                   "plain_requests_not_200": sum(t["count"] for t in probe["part_a_plain_requests"]["tally"] if t["status"] != 200)}
        z = e[name]
        lines += [f"- Zone-lookup probe, {label} (`zone-download-probe/{name}/`): {z['plain_requests']} plain requests "
                  f"({z['plain_requests_not_200']} not 200), then {z['calls']} calls of the pipeline's download: "
                  f"{z['right_file']} ended with the right file, {z['failed']} failed"
                  + (f" (HTTP {', '.join(map(str, z['failed_http_answers']))}; the failures spanned {z['failed_calls_span_s']:g} s)" if z["failed"] else "")
                  + f", {z['needed_a_retry']} needed a retry."]

    first_alerts = load(RESULTS / "alerts-first-attempt" / "contract-faults.json")
    if first_alerts:
        exercised = sorted({sum(1 for x in rep["faults"].values() if x["validate_state"] is not None) for rep in first_alerts["repeats"]})
        e["alerts_first_attempt"] = {"repeats": len(first_alerts["repeats"]), "faults_per_repeat": len(first_alerts["fault_to_dag_month"]),
                                     "faults_that_ran_per_repeat": exercised}
        a = e["alerts_first_attempt"]
        lines += [f"- First alerts attempt (`alerts-first-attempt/`): {a['repeats']} repeats offering {a['faults_per_repeat']} broken files "
                  f"to one backfill; the number of them that actually ran per repeat: {', '.join(map(str, a['faults_that_ran_per_repeat']))} "
                  f"(`airflow dags backfill` stops at the first failed task unless `--continue-on-failures` is given)."]
    lines.append("")
    out["earlier_attempts"] = e


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
    earlier_attempts_section(out, lines)
    write_json(RESULTS / "summary.json", out)
    (RESULTS / "SUMMARY.md").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print("wrote", RESULTS / "SUMMARY.md")


if __name__ == "__main__":
    main()
