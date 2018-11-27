
In the simple example below, iteration is used to display the names and values of the attributes of each entity in the simple table or table expression ``tab``.

.. code-block:: python

    for entity in tab:
        print(entity)

This example illustrates the function of the iterator: DataJoint iterates through the whole table expression, returning the entire entity during each step.
In this case, each entity will be returned as a ``dict`` containing all attributes.

At the start of the above loop, DataJoint internally fetches only the primary keys of the entities in ``tab``.
Since only the primary keys are needed to distinguish between entities, DataJoint can then iterate over the list of primary keys to execute the loop.
At each step of the loop, DataJoint uses a single primary key to fetch an entire entity for use in the iteration, such that ``print(entity)`` will print all attributes of each entity.
By first fetching only the primary keys and then fetching each entity individually, DataJoint saves memory at the cost of network overhead.
This can be particularly useful for tables containing large amounts of data in secondary attributes.

The memory savings of the above syntax may not be worth the additional network overhead in all cases, such as for tables with little data stored as secondary attributes.
In the example below, DataJoint fetches all of the attributes of each entity in a single call and then iterates over the list of entities stored in memory.

.. code-block:: python

    for entity in tab.fetch(as_dict=True):
        print(entity)
