class test:
    @property
    def x(self):
        return self._x + 1

    def __init__(self):
        self._y = 10
        self._x = 5
        print self.x

