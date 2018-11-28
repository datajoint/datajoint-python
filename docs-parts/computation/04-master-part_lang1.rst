
In Python, the master-part relationship is expressed by making the part a nested class of the master.
The part is subclassed from ``dj.Part`` and does not need the ``@schema`` decorator.


.. code-block:: python

    @schema
    class Segmentation(dj.Computed):
        definition = """  # image segmentation
        -> Image
        """

        class ROI(dj.Part):
            definition = """  # Region of interest resulting from segmentation
            -> Segmentation
            roi  : smallint   # roi number
            ---
            roi_pixels  : longblob   #  indices of pixels
            roi_weights : longblob   #  weights of pixels
            """

        def make(self, key):
            image = (Image & key).fetch1['image']
            self.insert1(key)
            count = itertools.count()
            Segmentation.ROI.insert(
                    dict(key, roi=next(count), roi_pixel=roi_pixels, roi_weights=roi_weights)
                    for roi_pixels, roi_weights in mylib.segment(image))
