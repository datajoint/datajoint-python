"""methods for generating previews of query expression results in python command line and Jupyter"""

import json

from .settings import config


def _format_object_display(json_data):
    """Format object metadata for display in query results."""
    if json_data is None:
        return "=OBJ[null]="
    if isinstance(json_data, str):
        try:
            json_data = json.loads(json_data)
        except (json.JSONDecodeError, TypeError):
            return "=OBJ=?"
    ext = json_data.get("ext")
    is_dir = json_data.get("is_dir", False)
    if ext:
        return f"=OBJ[{ext}]="
    elif is_dir:
        return "=OBJ[folder]="
    else:
        return "=OBJ[file]="


def _get_blob_placeholder(heading, field_name, html_escape=False):
    """Get display placeholder for a blob/json field based on its codec."""
    from .errors import DataJointError

    attr = heading.attributes.get(field_name)
    if attr is None:
        raise DataJointError(f"Field '{field_name}' not found in heading")
    if attr.codec is not None:
        name = attr.codec.name
        if html_escape:
            return f"&lt;{name}&gt;"
        return f"<{name}>"
    if attr.json:
        return "json"
    return "bytes"


def preview(query_expression, limit, width):
    heading = query_expression.heading
    rel = query_expression.proj(*heading.non_blobs)
    # Object fields use codecs - not specially handled in simplified model
    object_fields = []
    if limit is None:
        limit = config["display.limit"]
    if width is None:
        width = config["display.width"]
    tuples = rel.to_arrays(limit=limit + 1)
    has_more = len(tuples) > limit
    tuples = tuples[:limit]

    # Fetch object field JSON data for display (raw JSON, not ObjectRef)
    object_data_list = []
    if object_fields:
        # Fetch primary key and object fields as dicts
        obj_rel = query_expression.proj(*object_fields)
        obj_tuples = obj_rel.to_arrays(limit=limit)
        for obj_tup in obj_tuples:
            obj_dict = {}
            for field in object_fields:
                if field in obj_tup.dtype.names:
                    obj_dict[field] = obj_tup[field]
            object_data_list.append(obj_dict)

    columns = heading.names

    def get_placeholder(f):
        if f in object_fields:
            return "=OBJ[.xxx]="
        return _get_blob_placeholder(heading, f)

    widths = {
        f: min(
            max([len(f)] + [len(str(e)) for e in tuples[f]] if f in tuples.dtype.names else [len(get_placeholder(f))]) + 4,
            width,
        )
        for f in columns
    }
    templates = {f: "%%-%d.%ds" % (widths[f], widths[f]) for f in columns}

    def get_display_value(tup, f, idx):
        if f in tup.dtype.names:
            return tup[f]
        elif f in object_fields and idx < len(object_data_list):
            return _format_object_display(object_data_list[idx].get(f))
        else:
            return _get_blob_placeholder(heading, f)

    return (
        " ".join([templates[f] % ("*" + f if f in rel.primary_key else f) for f in columns])
        + "\n"
        + " ".join(["+" + "-" * (widths[column] - 2) + "+" for column in columns])
        + "\n"
        + "\n".join(" ".join(templates[f] % get_display_value(tup, f, idx) for f in columns) for idx, tup in enumerate(tuples))
        + ("\n   ...\n" if has_more else "\n")
        + (" (Total: %d)\n" % len(rel) if config["display.show_tuple_count"] else "")
    )


def repr_html(query_expression):
    heading = query_expression.heading
    rel = query_expression.proj(*heading.non_blobs)
    # Object fields use codecs - not specially handled in simplified model
    object_fields = []
    tuples = rel.to_arrays(limit=config["display.limit"] + 1)
    has_more = len(tuples) > config["display.limit"]
    tuples = tuples[0 : config["display.limit"]]

    # Fetch object field JSON data for display (raw JSON, not ObjectRef)
    object_data_list = []
    if object_fields:
        obj_rel = query_expression.proj(*object_fields)
        obj_tuples = obj_rel.to_arrays(limit=config["display.limit"])
        for obj_tup in obj_tuples:
            obj_dict = {}
            for field in object_fields:
                if field in obj_tup.dtype.names:
                    obj_dict[field] = obj_tup[field]
            object_data_list.append(obj_dict)

    def get_html_display_value(tup, name, idx):
        if name in tup.dtype.names:
            return tup[name]
        elif name in object_fields and idx < len(object_data_list):
            return _format_object_display(object_data_list[idx].get(name))
        else:
            return _get_blob_placeholder(heading, name, html_escape=True)

    css = """
    <style type="text/css">
        .Table{
            border-collapse:collapse;
        }
        .Table th{
            background: #A0A0A0; color: #ffffff; padding:2px 4px; border:#f0e0e0 1px solid;
            font-weight: normal; font-family: monospace; font-size: 75%; text-align: center;
        }
        .Table th p{
            margin: 0;
        }
        .Table td{
            padding:2px 4px; border:#f0e0e0 1px solid; font-size: 75%;
        }
        .Table tr:nth-child(odd){
            background: #ffffff;
            color: #000000;
        }
        .Table tr:nth-child(even){
            background: #f3f1ff;
            color: #000000;
        }
        #primary {
            font-weight: bold;
            color: black;
        }
        #nonprimary {
            font-weight: normal;
            color: white;
        }

        /* Dark mode support */
        @media (prefers-color-scheme: dark) {
            .Table th{
                background: #4a4a4a; color: #ffffff; border:#555555 1px solid; text-align: center;
            }
            .Table td{
                border:#555555 1px solid;
            }
            .Table tr:nth-child(odd){
                background: #2d2d2d;
                color: #e0e0e0;
            }
            .Table tr:nth-child(even){
                background: #3d3d3d;
                color: #e0e0e0;
            }
            #primary {
                color: #bd93f9;
            }
            #nonprimary {
                color: #e0e0e0;
            }
        }
    </style>
    """
    head_template = """<span id="{primary}" title="{comment}">{column}</span>"""
    return """
    {css}
    {title}
        <div style="max-height:1000px;max-width:1500px;overflow:auto;">
        <table border="1" class="Table">
            <thead> <tr> <th> {head} </th> </tr> </thead>
            <tbody> <tr> {body} </tr> </tbody>
        </table>
        {ellipsis}
        {count}</div>
        """.format(
        css=css,
        title="",  # Table comment not shown in preview; available via describe()
        head="</th><th>".join(
            head_template.format(
                column=c,
                comment=heading.attributes[c].comment,
                primary=("primary" if c in query_expression.primary_key else "nonprimary"),
            )
            for c in heading.names
        ),
        ellipsis="<p>...</p>" if has_more else "",
        body="</tr><tr>".join(
            [
                "\n".join(["<td>%s</td>" % get_html_display_value(tup, name, idx) for name in heading.names])
                for idx, tup in enumerate(tuples)
            ]
        ),
        count=(("<p>Total: %d</p>" % len(rel)) if config["display.show_tuple_count"] else ""),
    )
