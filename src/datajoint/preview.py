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


def preview(query_expression, limit, width):
    heading = query_expression.heading
    rel = query_expression.proj(*heading.non_blobs)
    # Object fields are AttributeTypes with adapters - not specially handled in simplified model
    object_fields = []
    if limit is None:
        limit = config["display.limit"]
    if width is None:
        width = config["display.width"]
    tuples = rel.fetch(limit=limit + 1, format="array")
    has_more = len(tuples) > limit
    tuples = tuples[:limit]

    # Fetch object field JSON data for display (raw JSON, not ObjectRef)
    object_data_list = []
    if object_fields:
        # Fetch primary key and object fields as dicts
        obj_rel = query_expression.proj(*object_fields)
        obj_tuples = obj_rel.fetch(limit=limit, format="array")
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
        return "=BLOB="

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
            return "=BLOB="

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
    # Object fields are AttributeTypes with adapters - not specially handled in simplified model
    object_fields = []
    info = heading.table_status
    tuples = rel.fetch(limit=config["display.limit"] + 1, format="array")
    has_more = len(tuples) > config["display.limit"]
    tuples = tuples[0 : config["display.limit"]]

    # Fetch object field JSON data for display (raw JSON, not ObjectRef)
    object_data_list = []
    if object_fields:
        obj_rel = query_expression.proj(*object_fields)
        obj_tuples = obj_rel.fetch(limit=config["display.limit"], format="array")
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
            return "=BLOB="

    css = """
    <style type="text/css">
        .Table{
            border-collapse:collapse;
        }
        .Table th{
            background: #A0A0A0; color: #ffffff; padding:4px; border:#f0e0e0 1px solid;
            font-weight: normal; font-family: monospace; font-size: 100%;
        }
        .Table td{
            padding:4px; border:#f0e0e0 1px solid; font-size:100%;
        }
        .Table tr:nth-child(odd){
            background: #ffffff;
            color: #000000;
        }
        .Table tr:nth-child(even){
            background: #f3f1ff;
            color: #000000;
        }
        /* Tooltip container */
        .djtooltip {
        }
        /* Tooltip text */
        .djtooltip .djtooltiptext {
            visibility: hidden;
            width: 120px;
            background-color: black;
            color: #fff;
            text-align: center;
            padding: 5px 0;
            border-radius: 6px;
            /* Position the tooltip text - see examples below! */
            position: absolute;
            z-index: 1;
        }
        #primary {
            font-weight: bold;
            color: black;
        }
        #nonprimary {
            font-weight: normal;
            color: white;
        }

        /* Show the tooltip text when you mouse over the tooltip container */
        .djtooltip:hover .djtooltiptext {
            visibility: visible;
        }
    </style>
    """
    head_template = """<div class="djtooltip">
                            <p id="{primary}">{column}</p>
                            <span class="djtooltiptext">{comment}</span>
                        </div>"""
    return """
    {css}
    {title}
        <div style="max-height:1000px;max-width:1500px;overflow:auto;">
        <table border="1" class="Table">
            <thead> <tr style="text-align: right;"> <th> {head} </th> </tr> </thead>
            <tbody> <tr> {body} </tr> </tbody>
        </table>
        {ellipsis}
        {count}</div>
        """.format(
        css=css,
        title="" if info is None else "<b>%s</b>" % info["comment"],
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
