# Blobs

Blob attributes store serialized Python objects in the database. DataJoint
automatically serializes objects on insert and deserializes them on fetch.

## Defining Blob Attributes

```python
@schema
class Recording(dj.Manual):
    definition = """
    recording_id : int
    ---
    signal : longblob        # numpy array
    metadata : longblob      # dictionary
    timestamps : longblob    # 1D array
    """
```

### Blob Sizes

| Type | Max Size | Use Case |
|------|----------|----------|
| `tinyblob` | 255 bytes | Small binary data |
| `blob` | 64 KB | Small arrays |
| `mediumblob` | 16 MB | Medium arrays |
| `longblob` | 4 GB | Large arrays, images |

Use `longblob` for most scientific data to avoid size limitations.

## Inserting Blobs

```python
import numpy as np

# Insert numpy arrays
Recording.insert1({
    'recording_id': 1,
    'signal': np.random.randn(10000, 64),  # 10k samples, 64 channels
    'metadata': {'sampling_rate': 30000, 'gain': 1.5},
    'timestamps': np.linspace(0, 10, 10000)
})
```

### Supported Types

DataJoint serializes these Python types:

**Scalars**
```python
data = {
    'int_val': 42,
    'float_val': 3.14159,
    'bool_val': True,
    'str_val': 'hello world',
}
```

**Collections**
```python
data = {
    'list_val': [1, 2, 3, 4, 5],
    'tuple_val': (1, 'a', 3.14),
    'set_val': {1, 2, 3},
    'dict_val': {'key1': 'value1', 'key2': [1, 2, 3]},
}
```

**NumPy Arrays**
```python
data = {
    'array_1d': np.array([1, 2, 3, 4, 5]),
    'array_2d': np.random.randn(100, 100),
    'array_3d': np.zeros((10, 256, 256)),  # e.g., video frames
    'complex_array': np.array([1+2j, 3+4j]),
    'structured': np.array([(1, 2.0), (3, 4.0)],
                           dtype=[('x', 'i4'), ('y', 'f8')]),
}
```

**Special Types**
```python
import uuid
from decimal import Decimal
from datetime import datetime, date

data = {
    'uuid_val': uuid.uuid4(),
    'decimal_val': Decimal('3.14159265358979'),
    'datetime_val': datetime.now(),
    'date_val': date.today(),
}
```

## Fetching Blobs

Blobs are automatically deserialized on fetch:

```python
# Fetch entire entity
record = (Recording & 'recording_id=1').fetch1()
signal = record['signal']  # numpy array
metadata = record['metadata']  # dict

# Fetch specific blob attribute
signal = (Recording & 'recording_id=1').fetch1('signal')
print(signal.shape)  # (10000, 64)
print(signal.dtype)  # float64

# Fetch multiple blobs
signal, timestamps = (Recording & 'recording_id=1').fetch1('signal', 'timestamps')
```

## External Storage

For large blobs, use external storage to avoid database bloat:

```python
@schema
class LargeData(dj.Manual):
    definition = """
    data_id : int
    ---
    large_array : blob@external  # stored outside database
    """
```

Configure external storage in settings:

```json
{
    "stores": {
        "external": {
            "protocol": "file",
            "location": "/data/blobs"
        }
    }
}
```

See [External Store](../admin/external-store.md) for configuration details.

## Compression

Blobs larger than 1 KiB are automatically compressed using zlib. This is
transparent to users—compression/decompression happens automatically.

```python
# Large array is compressed automatically
large_data = np.random.randn(1000000)  # ~8 MB uncompressed
Table.insert1({'data': large_data})  # Stored compressed
fetched = Table.fetch1('data')  # Decompressed automatically
```

## Performance Tips

### Use Appropriate Data Types

```python
# Good: use float32 when float64 precision isn't needed
signal = signal.astype(np.float32)  # Half the storage

# Good: use appropriate integer sizes
counts = counts.astype(np.uint16)  # If values < 65536
```

### Avoid Storing Redundant Data

```python
# Bad: store computed values that can be derived
Recording.insert1({
    'signal': signal,
    'mean': signal.mean(),  # Can be computed from signal
    'std': signal.std(),    # Can be computed from signal
})

# Good: compute on fetch
signal = Recording.fetch1('signal')
mean, std = signal.mean(), signal.std()
```

### Consider Chunking Large Data

```python
# For very large data, consider splitting into chunks
@schema
class VideoFrame(dj.Manual):
    definition = """
    -> Video
    frame_num : int
    ---
    frame : longblob
    """

# Store frames individually rather than entire video
for i, frame in enumerate(video_frames):
    VideoFrame.insert1({'video_id': 1, 'frame_num': i, 'frame': frame})
```

## MATLAB Compatibility

DataJoint's blob format is compatible with MATLAB's mYm serialization,
allowing data sharing between Python and MATLAB pipelines:

```python
# Data inserted from Python
Table.insert1({'data': np.array([[1, 2], [3, 4]])})
```

```matlab
% Fetched in MATLAB
data = fetch1(Table, 'data');
% data is a 2x2 matrix
```

## Common Patterns

### Store Model Weights

```python
@schema
class TrainedModel(dj.Computed):
    definition = """
    -> TrainingRun
    ---
    weights : longblob
    architecture : varchar(100)
    accuracy : float
    """

    def make(self, key):
        model = train_model(key)
        self.insert1(dict(
            key,
            weights=model.get_weights(),
            architecture=model.name,
            accuracy=evaluate(model)
        ))
```

### Store Image Data

```python
@schema
class Image(dj.Manual):
    definition = """
    image_id : int
    ---
    pixels : longblob      # HxWxC array
    format : varchar(10)   # 'RGB', 'RGBA', 'grayscale'
    """

# Insert image
import imageio
img = imageio.imread('photo.png')
Image.insert1({'image_id': 1, 'pixels': img, 'format': 'RGB'})

# Fetch and display
import matplotlib.pyplot as plt
pixels = (Image & 'image_id=1').fetch1('pixels')
plt.imshow(pixels)
```

### Store Time Series

```python
@schema
class TimeSeries(dj.Imported):
    definition = """
    -> Recording
    ---
    data : longblob          # NxT array (N channels, T samples)
    sampling_rate : float    # Hz
    start_time : float       # seconds
    """

    def make(self, key):
        data, sr, t0 = load_recording(key)
        self.insert1(dict(key, data=data, sampling_rate=sr, start_time=t0))
```

## Limitations

- Blob content is opaque to SQL queries (can't filter by array values)
- Large blobs increase database backup size
- Consider [object type](object.md) for very large files or cloud storage
- Avoid storing objects with external references (file handles, connections)
