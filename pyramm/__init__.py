__version__ = "1.7"

from . import api, ops  # noqa


if __name__ == "__main__":
    # Connect and build centreline object:
    conn = api.Connection()
    centreline = conn.centreline()

    # Download the surface table
    top_surface = conn.top_surface().reset_index()

    # Append geometry:
    top_surface = centreline.append_geometry(top_surface)

    top_surface
