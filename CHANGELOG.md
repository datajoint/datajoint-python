## Release notes
### 0.3.2.   
* Fixed issue #223:  `insert` can insert relations without fetching
* ERD() now takes the `context` argument, which specifies in which context to look for classes. The default is taken from the argument (schema or relation).
* ERD.draw() no longer has the `prefix` argument: class names are shown as found in the context.
