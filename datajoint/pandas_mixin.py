
try:
    import pandas as pd
except ImportError:
    class PandasMixin:
        pass
else:

    class PandasMixin:
        """
        Mix-in class to add pandas access functionality to a datajoint query expression
        """

        def get_df(self, **kwargs):
            """
            fetch query result as a pandas dataframe
            """
            kwargs['as_dict'] = False
            return pd.DataFrame(self.fetch(**kwargs))

        @property
        def df(self):
            return self.get_df()

        def get_head(self, n=25):
            """
            Fetch the head of the table (first n entries), return in a pandas.DataFrame
            """
            return self.get_df(order_by=self.primary_key, limit=n)

        @property
        def head(self):
            return self.get_head()

        def get_tail(self, n=25):
            """
            Fetch the tail of the table (last n entries), return in a pandas.DataFrame
            """
            return self.get_df(order_by=(s + ' DESC' for s in self.primary_key), limit=n)

        @property
        def tail(self):
            return self.get_tail()
