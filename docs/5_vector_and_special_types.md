## Vector and Special Types

This page documents the special data types exposed by `nebulagraph_python` for vectors and durations. These types can be constructed directly for client-side use.

- `NVector`: fixed-dimension embedding/feature vectors of floats
- `NDuration`: ISO-8601–like duration type supporting both month-based and time-based representations

Both are defined in `src/nebulagraph_python/py_data_types.py`.

### NVector

`NVector` represents a fixed-size vector of `float` values.

Key characteristics:
- Indexing and `len()` are supported
- String/`repr` are concise and readable: `NVector([0.1, 0.2])`

API:
- Constructor: `NVector(values: list[float])`
- Properties/Methods:
  - `dimension: int` — property returning the length
  - `get_values() -> list[float]`
  - `get_dimension() -> int`
  - `__len__() -> int`
  - `__getitem__(index: int) -> float` — bounds-checked
  - `__eq__(other) -> bool` — element-wise equality

Examples:
```python
from nebulagraph_python.client.client import NebulaClient
from nebulagraph_python.py_data_types import NVector

# Connect (adjust hosts/credentials to your environment)
cli = NebulaClient(hosts=["127.0.0.1:9669"], username="root", password="NebulaGraph01")

# RETURN a vector and read it from the result
res = cli.execute_py("RETURN vector<3, float>([1, 2, 3]) AS vec")
# Or pass the value to GQL literal
# vec = NVector([0.1, 0.2, 0.3])
# res = cli.execute_py("RETURN {{ vec }} AS vec", {"vec": NVector([0.1, 0.2, 0.3])}) 
row = res.one()
returned_vec = row["vec"].cast()          # -> NVector([0.1, 0.2, 0.3])
print(
  isinstance(returned_vec, NVector),
  returned_vec,
  returned_vec.get_dimension(),
  returned_vec.get_values()
)

cli.close()
```

### NDuration

`NDuration` represents a duration with support for both month-based and time-based forms.
- If `months != 0`, the instance is month-based (`is_month_based = True`), and `years`/`months` are derived from the `months` argument.
- If `months == 0`, the instance is time-based (days default to 0 in current implementation) and uses the `seconds` and `microseconds` arguments to derive `hour`, `minute`, `second`, `microsec`.

The `__str__` produces an ISO-8601–like string:
- Month-based examples: `P1Y2M`, `P0M`
- Time-based examples: `PT0S`, `PT1H2M3S`, `PT3.5S`, `PT-0.000123S`

API:
- Constructor: `NDuration(seconds: int, microseconds: int, months: int)`
- Properties/Methods:
  - `is_month_based: bool`
  - `get_year() -> int`, `get_month() -> int`, `get_day() -> int`
  - `get_hour() -> int`, `get_minute() -> int`, `get_second() -> int`, `get_microsecond() -> int`
  - `__str__() -> str` — ISO-like duration

Examples:
```python
from nebulagraph_python.client.client import NebulaClient
from nebulagraph_python.py_data_types import NDuration

cli = NebulaClient(hosts=["127.0.0.1:9669"], username="root", password="Nebula.123")

# RETURN a duration literal from the server and read it
# Adjust the literal to your NebulaGraph version if needed
res = cli.execute_py('RETURN duration("PT3.5S") AS dur')
row = res.one().as_primitive()
returned_dur = row["dur"]          # -> NDuration instance
print(
    isinstance(returned_dur, NDuration), # True
    returned_dur, # PT3.5S
)

cli.close()
```
