# pyramm

Python wrapper for RAMM API.

**Users must have their own login for the RAMM database.**

## Issues

Please submit an issue if you find a bug or have an idea for an improvement.

## Installation

```
pip install pyramm
```

## Initialise

You must first initialise the connection to the RAMM API as follows. Note that the
`database` argument defaults to `"SH New Zealand"` if it is not provided.

```
from pyramm.api import Connection
conn = Connection(username, password, database="SH New Zealand")
```

Alternatively the username and password can be stored in file called `.pyramm.ini`. This
file must be saved in the users home directory (`"~"` on linux) and contain the following:

```
[RAMM]
USERNAME = username
PASSWORD = password
```

You are then able to initialise the RAMM API connection without providing your login
credentials each time.

```
from pyramm.api import Connection
conn = Connection()
```

## Table and column names

A list of available tables can be accessed using:

```
table_names = conn.table_names()
```

A list of columns for a given table can be accessed using:

```
column_names = conn.column_names(table_name)
```

## Table data

Some methods are attached to the `Connection` object to provide convenient access to
selected RAMM tables. These helper methods implement some additional filtering (exposed
as method arguments) and automatically set the DataFrame index to the correct table
column(s).

Tables not listed in the sections below can be accessed using the general `get_data()`
method:

```
df = conn.get_data(table_name)
```

### General tables:
```
roadnames = conn.roadnames()
```
```
carrway = conn.carr_way(road_id=None)
```
```
c_surface = conn.c_surface(road_id=None)
```
```
top_surface = conn.top_surface()
```
```
surf_material = conn.surf_material()
```
```
surf_category = conn.surf_category()
```
```
minor_structure = conn.minor_structure()
```

### HSD tables:

```
hsd_roughness = conn.hsd_roughness(road_id, latest=True, survey_year=None)
```
```
hsd_roughness_hdr = conn.hsd_roughness_hdr()
```
```
hsd_rutting = conn.hsd_rutting(road_id, latest=True, survey_year=None)
```
```
hsd_rutting_hdr = conn.hsd_rutting_hdr()
```
```
hsd_texture = conn.hsd_texture(road_id, latest=True, survey_year=None)
```
```
hsd_texture_hdr = conn.hsd_texture_hdr()
```

## Centreline

The `Centreline` object is provided to:
 - assist with generating geometry for table entries (based on `road_id`, `start_m` and
`end_m` values),
 <!-- - find the nearest geometry element to give a point (`latitude`, `longitude`),
 - find the displacement (in metres) along the nearest geometry element given a point
(`latitude`, `longitude`). -->

The base geometry used by the `Centreline` object is derived from the `carr_way` table.

### Create a Centreline instance:

```
centreline = conn.centreline()
```

### Append geometry to table:

For a table containing `road_id`, `start_m` and `end_m` columns, the geometry can be
appended using the `append_geometry()` method:

```
df = centreline.append_geometry(df, geometry_type="wkt")
```

The `geometry_type` argument defaults to `"wkt"`. This will provide a
[WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry)
LineString for each row.

Alternatively, `geometry_type` can be set to `"coord"` to append
a `northing` and `easting` column to the DataFrame.
