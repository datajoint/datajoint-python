A :ref:`part table <master-part>` is usually removed as a consequence of calling ``drop`` on its master table.
To enforce this workflow, calling ``drop`` directly on a part table produces an error.
In some cases, it may be necessary to override this behavior.
To remove a part table without removing its master, use the argument ``force=True``.
