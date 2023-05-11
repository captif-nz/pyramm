# pyramm

<img align="right" src="https://github.com/captif-nz/pyramm/actions/workflows/push.yml/badge.svg">


Python wrapper for the [RAMM API](https://api.ramm.com/v1/documentation/index).

**Users must have their own login for the RAMM database.**

## Installation

```bash
pip install pyramm
```

## Issues

Please submit an issue if you find a bug or have an idea for an improvement.

## Initialise

You must first initialise the connection to the RAMM API as follows. Note that the
`database` argument defaults to `"SH New Zealand"` if it is not provided.

```python
from pyramm.api import Connection
conn = Connection(username, password, database="SH New Zealand")
```

Alternatively the username and password can be stored in file called `.pyramm.ini`. This
file must be saved in the users home directory (`"~"` on linux) and contain the following:

```ini
[RAMM]
USERNAME = username
PASSWORD = password
```

You are then able to initialise the RAMM API connection without providing your login
credentials each time.

```python
from pyramm.api import Connection
conn = Connection()
```

## Table and column names

A list of available tables can be accessed using:

```python
table_names = conn.table_names()
```

A list of columns for a given table can be accessed using:

```python
column_names = conn.column_names(table_name)
```

## Table data

Some methods are attached to the `Connection` object to provide convenient access to
selected RAMM tables. These helper methods implement some additional filtering (exposed
as method arguments) and automatically set the DataFrame index to the correct table
column(s).

Tables not listed in the sections below can be accessed using the general `get_data()`
method:

```python
df = conn.get_data(table_name)
```

### General tables:
```python
roadnames = conn.roadnames()
```
```python
carrway = conn.carr_way(road_id=None)
```
```python
c_surface = conn.c_surface(road_id=None)
```
```python
top_surface = conn.top_surface()
```
```python
surf_material = conn.surf_material()
```
```python
surf_category = conn.surf_category()
```
```python
minor_structure = conn.minor_structure()
```

### HSD tables:

```python
hsd_roughness = conn.hsd_roughness(road_id, latest=True, survey_year=None)
```
```python
hsd_roughness_hdr = conn.hsd_roughness_hdr()
```
```python
hsd_rutting = conn.hsd_rutting(road_id, latest=True, survey_year=None)
```
```python
hsd_rutting_hdr = conn.hsd_rutting_hdr()
```
```python
hsd_texture = conn.hsd_texture(road_id, latest=True, survey_year=None)
```
```python
hsd_texture_hdr = conn.hsd_texture_hdr()
```

## Centreline

The `Centreline` object is provided to:
 - assist with generating geometry for table entries (based on `road_id`, `start_m` and
`end_m` values),
 <!-- - find the nearest geometry element to give a point (`latitude`, `longitude`), -->
 - find the displacement (in metres) along the nearest geometry element given a point
(`latitude`, `longitude`).

The base geometry used by the `Centreline` object is derived from the `carr_way` table.

### Create a Centreline instance:

```python
centreline = conn.centreline()
```

### Append geometry to table:

For a table containing `road_id`, `start_m` and `end_m` columns, the geometry can be
appended using the `append_geometry()` method:

```python
df = centreline.append_geometry(df, geometry_type="wkt")
```

The `geometry_type` argument defaults to `"wkt"`. This will provide a
[WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry)
LineString for each row.

Alternatively, `geometry_type` can be set to `"coord"` to append
a `northing` and `easting` column to the DataFrame.

### Find carriageway and position from point coordinates:

The carriageway and position information (e.g. Rs/Rp) can be determined for a point coordinate
using the `position()` method:

```python
point = Point((172.618567, -43.441594))  # Shapely Point object
position = centreline.position(point, point_crs=4326, road_id=None)
```

The point coordinate reference system defaults to WGS84 but can be adjusted using the
`point_crs` argument. The value must be an integer corresponing to the
[EPSG code](https://epsg.io/) (e.g. `4326` for WGS84).

If the `road_id` argument is provided then the position will be determined only for the
specified road. Otherwise the position will be determined for the nearest road.

#### Partial centreline

Sometimes it is necessary to match only to selected parts of the RAMM centreline. In this
case a partial centreline can be generated and used for the matching:

```python
# Generate a partial centreline containing only road_id 3656 between route position 10m
# and 100m:
partial_centreline = conn.centreline(lengths={3656: [10, 100]})

point = Point((172.608406, -43.451023))
position = partial_centreline.position(point)
```

The `lengths` argument is a Python dictionary containing `road_id` keys and start/end
position pair values. Some examples include:

- `{3656: None}` includes the entire centreline for road_id 3656.
- `{3656: [10, 100]}` includes only the section of centreline for road_id 3656 between route position 10m and 100m.
- `{3656: [500, None]}` includes only the section of centreline for road_id 3656 from route position 500m.
