""" methods for generating previews of query expression results in python command line and Jupyter """

from .settings import config


def preview(query_expression, limit, width):
    heading = query_expression.heading
    rel = query_expression.proj(*heading.non_blobs)
    if limit is None:
        limit = config['display.limit']
    if width is None:
        width = config['display.width']
    tuples = rel.fetch(limit=limit + 1, format="array")
    has_more = len(tuples) > limit
    tuples = tuples[:limit]
    columns = heading.names
    widths = {f: min(max([len(f)] +
                         [len(str(e)) for e in tuples[f]] if f in tuples.dtype.names else [len('=BLOB=')]) + 4, width) for f
              in columns}
    templates = {f: '%%-%d.%ds' % (widths[f], widths[f]) for f in columns}
    return (
            ' '.join([templates[f] % ('*' + f if f in rel.primary_key else f) for f in columns]) + '\n' +
            ' '.join(['+' + '-' * (widths[column] - 2) + '+' for column in columns]) + '\n' +
            '\n'.join(' '.join(templates[f] % (tup[f] if f in tup.dtype.names else '=BLOB=')
                               for f in columns) for tup in tuples) +
            ('\n   ...\n' if has_more else '\n') +
            (' (Total: %d)\n' % len(rel) if config['display.show_tuple_count'] else ''))


def repr_html(query_expression):
    heading = query_expression.heading
    rel = query_expression.proj(*heading.non_blobs)
    info = heading.table_status
    tuples = rel.fetch(limit=config['display.limit'] + 1, format='array')
    has_more = len(tuples) > config['display.limit']
    tuples = tuples[0:config['display.limit']]

    css = """
    <style type="text/css">
        .Relation{
            border-collapse:collapse;
        }
        .Relation th{
            background: #A0A0A0; color: #ffffff; padding:4px; border:#f0e0e0 1px solid;
            font-weight: normal; font-family: monospace; font-size: 100%;
        }
        .Relation td{
            padding:4px; border:#f0e0e0 1px solid; font-size:100%;
        }
        .Relation tr:nth-child(odd){
            background: #ffffff;
        }
        .Relation tr:nth-child(even){
            background: #f3f1ff;
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
        <table border="1" class="Relation">
            <thead> <tr style="text-align: right;"> <th> {head} </th> </tr> </thead>
            <tbody> <tr> {body} </tr> </tbody>
        </table>
        {ellipsis}
        {count}</div>
        """.format(
        css=css,
        title="" if info is None else "<b>%s</b>" % info['comment'],
        head='</th><th>'.join(
            head_template.format(column=c, comment=heading.attributes[c].comment,
                                 primary='primary' if c in query_expression.primary_key else 'nonprimary') for c in
            heading.names),
        ellipsis='<p>...</p>' if has_more else '',
        body='</tr><tr>'.join(
            ['\n'.join(['<td>%s</td>' % (tup[name] if name in tup.dtype.names else '=BLOB=')
                        for name in heading.names])
             for tup in tuples]),
        count=('<p>Total: %d</p>' % len(rel)) if config['display.show_tuple_count'] else '')
