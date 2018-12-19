To enforce this workflow, calling ``delete`` directly on a part table produces an error.
In some cases, it may be necessary to override this behavior.
To remove entities from a part table without calling ``delete`` master, use the argument ``force=True``.
