# External Data

## File Attachment Datatype

### Configuration & Usage

Corresponding to issue 
[#480](https://github.com/datajoint/datajoint-python/issues/480), 
the `attach` attribute type allows users to `attach` files into DataJoint
schemas as DataJoint-managed files. This is in contrast to traditional `blobs`
which are encodings of programming language data structures such as arrays.

The functionality is modeled after email attachments, where users `attach`
a file along with a message and message recipients have access to a
copy of that file upon retrieval of the message.

For DataJoint `attach` attributes, DataJoint will copy the input
file into a DataJoint store, hash the file contents, and track
the input file name. Subsequent `fetch` operations will transfer a
copy of the file to the local directory of the Python process and
return a pointer to it's location for subsequent client usage. This
allows arbitrary files to be `uploaded` or `attached` to a DataJoint
schema for later use in processing. File integrity is preserved by
checksum comparison against the attachment data and verifying the contents
during retrieval.

For example, given a `localattach` store:

```python
dj.config['stores'] = {
  'localattach': {
    'protocol': 'file',
    'location': '/data/attach'
  }
}
```

A `ScanAttachment` table can be created:

```python
@schema
class ScanAttachment(dj.Manual):
    definition = """
    -> Session
    ---
    scan_image:    attach@localattach  # attached image scans
    """
```

Files can be added using an insert pointing to the source file:

```python
>>> ScanAttachment.insert1((0, '/input/image0.tif'))
```

And then retrieved to the current directory using `fetch`:

```python
>>> s0 = (ScanAttachment & {'session_id': 0}).fetch1()
>>> s0
{'session_id': 0, 'scan_image': './image0.tif'}
>>> fh = open(s0['scan_image'], 'rb')
>>> fh
<_io.BufferedReader name='./image0.tif')
```

<!-- TODO: explain how filename collisions are handled -->
