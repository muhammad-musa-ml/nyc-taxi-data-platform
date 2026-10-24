"""The lineage renderer, on a small hand-made dbt manifest."""
from __future__ import annotations

import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("lineage", Path(__file__).resolve().parent.parent / "scripts" / "lineage.py")
lineage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lineage)

MANIFEST = {
    "metadata": {"dbt_version": "1.x", "generated_at": "2026-01-01T00:00:00Z"},
    "sources": {
        "source.t.raw.trips": {"resource_type": "source", "source_name": "raw", "name": "trips",
                               "description": "Rows that passed.", "original_file_path": "models/staging/_sources.yml"},
    },
    "nodes": {
        "model.t.stg_trips": {"resource_type": "model", "name": "stg_trips", "config": {"materialized": "view"},
                              "original_file_path": "models/staging/stg_trips.sql", "description": "Typed view. More text.",
                              "depends_on": {"nodes": ["source.t.raw.trips"]}},
        "seed.t.dim_pay": {"resource_type": "seed", "name": "dim_pay", "config": {},
                           "original_file_path": "seeds/dim_pay.csv", "description": "", "depends_on": {"nodes": []}},
        "model.t.fct_trips": {"resource_type": "model", "name": "fct_trips", "config": {"materialized": "incremental"},
                              "original_file_path": "models/marts/fct_trips.sql", "description": "The fact | table.",
                              "depends_on": {"nodes": ["model.t.stg_trips", "seed.t.dim_pay"]}},
        "test.t.not_null_fct": {"resource_type": "test", "attached_node": "model.t.fct_trips",
                                "depends_on": {"nodes": ["model.t.fct_trips"]}},
        "test.t.not_null_fct2": {"resource_type": "test", "attached_node": "model.t.fct_trips",
                                 "depends_on": {"nodes": ["model.t.fct_trips"]}},
        "test.t.singular": {"resource_type": "test", "attached_node": None,
                            "depends_on": {"nodes": ["model.t.fct_trips", "model.t.stg_trips"]}},
    },
}


def test_edges_follow_depends_on_and_skip_tests():
    nodes, edges, tests = lineage.collect(MANIFEST)
    assert set(nodes) == {"source.t.raw.trips", "model.t.stg_trips", "seed.t.dim_pay", "model.t.fct_trips"}
    assert edges == sorted([("source.t.raw.trips", "model.t.stg_trips"), ("model.t.stg_trips", "model.t.fct_trips"),
                            ("seed.t.dim_pay", "model.t.fct_trips")])
    assert tests["model.t.fct_trips"] == 2 and tests["model.t.stg_trips"] == 0


def test_rendered_page_has_layers_edges_and_a_row_per_model():
    page = lineage.render(MANIFEST)
    assert "flowchart LR" in page
    for title in ("Sources", "Seeds", "Staging", "Marts"):
        assert f'["{title}"]' in page
    assert "source_t_raw_trips --> model_t_stg_trips" in page
    assert "| `fct_trips` | marts | incremental | 2 | The fact / table. |" in page
    assert "| `stg_trips` | staging | view | 0 | Typed view. |" in page
    assert "| `raw.trips` | source | source | 0 | Rows that passed. |" in page
    assert "the quarantine reasons): 1." in page
    assert "Generic tests attached to these models: 2." in page
