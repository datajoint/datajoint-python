
.. code-block:: python

    @schema
    class FilteredImage(dj.Computed):
        definition = """
        # Filtered image
        -> Image
        ---
        filtered_image : longblob
        """

        def make(self, key):
            img = (test.Image & key).fetch1['image']
            key['filtered_image'] = myfilter(img)
            self.insert(key)

The ``make`` method receives one argument: the dict ``key`` containing the primary key value of an element of :ref:`key source <keysource>` to be worked on.  
