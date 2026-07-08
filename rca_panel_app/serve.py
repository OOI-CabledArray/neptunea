"""Console entry point: launch the QC Explorer with `qc-explorer`."""
from pathlib import Path

import click

APP = Path(__file__).parent / "qc_explorer.py"


@click.command()
@click.option("--port", default=5006, show_default=True, help="Port to serve on")
@click.option("--show/--no-show", default=True, show_default=True, help="Open a browser tab")
def main(port, show):
    import panel as pn
    pn.serve(str(APP), port=port, show=show)


if __name__ == "__main__":
    main()
