# Blobs

DataJoint provides functionality for serializing and deserializing complex data types
into binary blobs for efficient storage and compatibility with MATLAB's mYm
serialization. This includes support for:

+ Basic Python data types (e.g., integers, floats, strings, dictionaries).
+ NumPy arrays and scalars.
+ Specialized data types like UUIDs, decimals, and datetime objects.

## Serialization and Deserialization Process

Serialization converts Python objects into a binary representation for efficient storage
within the database. Deserialization converts the binary representation back into the
original Python object.

Blobs over 1 KiB are compressed using the zlib library to reduce storage requirements.

## Supported Data Types

DataJoint supports the following data types for serialization:

+ Scalars: Integers, floats, booleans, strings.
+ Collections: Lists, tuples, sets, dictionaries.
+ NumPy: Arrays, structured arrays, and scalars.
+ Custom Types: UUIDs, decimals, datetime objects, MATLAB cell and struct arrays.
