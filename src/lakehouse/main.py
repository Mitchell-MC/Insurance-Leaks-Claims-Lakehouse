"""CLI entry point for orchestrating lakehouse pipeline stages."""

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    """Builds the top-level CLI argument parser.

    Returns:
        argparse.ArgumentParser: Parser with a `stage` positional argument
            selecting which pipeline stage to run.
    """
    parser = argparse.ArgumentParser(
        prog="lakehouse",
        description="Insurance Claims Leakage & Catastrophe Response Analytics Lakehouse CLI",
    )
    parser.add_argument(
        "stage",
        choices=["ingest", "silver", "process", "gold"],
        help="Pipeline stage to run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parses CLI arguments and dispatches to the requested pipeline stage.

    Args:
        argv (list[str] | None): Command-line arguments, defaults to sys.argv.

    Returns:
        int: Process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    print(f"Stage '{args.stage}' is not yet implemented.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
