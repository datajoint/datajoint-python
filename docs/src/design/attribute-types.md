# Datatypes

Throughout the DataJoint ecosystem, there are several datatypes that are used to define
tables with cross-platform support (i.e. Python, MATLAB). It is important to understand
these types as they can have implications in the queries you form and the capacity of
their storage.

## Standard Types

These types are largely wrappers around existing types in the current 
[query backend](../../ref-integrity/query-backend) for [data pipelines](../../getting-started/data-pipelines).

### Common Types

|           Datatype                     | Description  |  Size   | Example | Range |
|              ---                       |     ---      |   ---   |   ---   |  ---  |
| <span id="int">int</span>              |   integer    | 4 bytes |   `8`   | -2<sup>31</sup> to 2<sup>31</sup>-1 |
| <span id="enum">enum</span>[^1]        |   category   |1-2 bytes| `M`, `F`| -2<sup>31</sup> to 2<sup>31</sup>-1 |
| <span id="datetime">datetime</span>[^2]| date and time in `YYYY-MM-DD HH:MM:SS` format | 5 bytes | `'2020-01-02 03:04:05'` | |
| <span id="varchar">varchar(N)</span>   | string of length *M*, up to *N* | *M* + 1-2 bytes| `text`| |
| <span id="float">float</span>[^3]      | floating point number | 4 bytes| `2.04`| 3.40E+38 to -1.17E-38, 0, and 1.17E-38 to 3.40E+38 |
| <span id="longblob">longblob</span>[^4]    | arbitrary numeric data| ≤ 4 GiB | | |

### Less Common Types

The following types add more specificity to the options above. Note that any integer
type can be unsigned, shifting their range from the listed ±2<sup>n</sup> to from 0 -
2<sup>n+1</sup>. Float and decimal types can be similarly unsigned

|           Datatype                     | Description  |  Size   | Example | Range |
|              ---                       |     ---      |   ---   |   ---   |  ---  |
| <span id="tiny-int">tinyint</span>     |tiny integer  | 1 byte  |   `2`   | -2<sup>7</sup> to 2<sup>7</sup>-1 |
| <span id="small-int">smallint</span>   |small integer | 2 bytes | `21,000`| -2<sup>15</sup> to 2<sup>15</sup>-1 |
| <span id="medium-int">mediumint</span> |medium integer| 3 bytes |`401,000`| -2<sup>23</sup> to 2<sup>23</sup>-1 |
| <span id="date">date</span>            |date          | 5 bytes | `'2020-01-02'` | |
| <span id="time">time</span>            |time          | 5 bytes | `'03:04:05'` | |
| <span id="datetime">datetime</span>[^5]|date and time | 5 bytes | `'2020-01-02 03:04:05'` | |
| <span id="char(N)">char(N)</span>      |string of exactly length *N*           | *N* bytes| `text` | |
| <span id="double">double</span>        |double-precision floating point number | 8 bytes  | | |
| <span id="decimalnf">decimal(N,F)</span>   |a fixed-point number with *N* total and *F* fractional digits | 4 bytes per 9 digits | | |
| <span id="tinyblob">tinyblob</span>[^4]    | arbitrary numeric data| ≲ 256 bytes | | |
| <span id="blob">blob</span>[^4]            | arbitrary numeric data| ≤ 64 KiB    | | |
| <span id="mediumblob">mediumblob</span>[^4]| arbitrary numeric data| ≤ 16 MiB    | | |

## Unique Types

|         Datatype            |      Description    |   Size   | Example |
|            ---              |          ---        |    ---   |   ---   |
| <span id="uuid">uuid</span> | a unique GUID value | 16 bytes | `6ed5ed09-e69c-466f-8d06-a5afbf273e61` |
| <span id="attach">attach</span> | file attachment | | |
| <span id="filepath">filepath</span> | path to external file | | |

## Unsupported Datatypes (for now)

- binary
- text
- longtext
- bit

For more information about datatypes, see 
[additional documentation](https://dev.mysql.com/doc/refman/5.6/en/data-types.html)

[^1]: *enum* datatypes can be useful to standardize spelling with limited categories,
but use with caution. *enum* should not be included in primary keys, as specified values
cannot be changed later.

[^2]: The default *datetime* value may be set to `CURRENT_TIMESTAMP`. 

[^3]: Because equality comparisons are error-prone, neither *float* nor *double* should
be used in primary keys. For these cases, consider *decimal*.

[^4]: Numeric arrays (e.g. matrix, image, structure) are compatible between MATLAB and
Python(NumPy). The *longblob* and other *blob* datatypes can be configured to store
data externally by using the `blob@store` syntax. For more information on storage limits
see [this article](https://en.wikipedia.org/wiki/Byte#Multiple-byte_units)

[^5]: Unlike *datetime*, a *timestamp* value will be adjusted to the local time zone.
