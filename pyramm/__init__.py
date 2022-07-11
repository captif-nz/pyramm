import warnings


with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="distutils Version classes are deprecated. Use packaging.version instead.",
    )
    from . import api, ops  # noqa
    from .logging import logger  # noqa
    from .version import __version__  # noqa


if __name__ == "__main__":
    # Connect and build centreline object:
    conn = api.Connection()
    centreline = conn.centreline()

    # Download the surface table
    top_surface = conn.top_surface().reset_index()

    # Append geometry:
    top_surface = centreline.append_geometry(top_surface)

    top_surface
