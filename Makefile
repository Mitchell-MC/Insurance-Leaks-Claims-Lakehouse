# Convenience wrapper only -- the underlying primitive stays four independent
# `docker compose run` invocations (see README.md) so any stage can be
# inspected/debugged on its own, mirroring infra/jobs.tf's explicit
# task-dependency chain rather than hiding it behind one opaque command.

.PHONY: run-pipeline
run-pipeline:
	docker compose run --rm lakehouse ingest --source historical
	docker compose run --rm lakehouse silver
	docker compose run --rm lakehouse process
	docker compose run --rm lakehouse gold
	docker compose run --rm lakehouse export
