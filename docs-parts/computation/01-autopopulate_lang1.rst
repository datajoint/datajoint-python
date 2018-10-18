
|python| Python

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
