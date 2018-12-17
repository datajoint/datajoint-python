The ``populate`` method accepts a number of optional arguments that provide more features and allow greater control over the method's behavior.

- ``restrictions`` - A list of restrictions, restricting as ``(tab.key_source & AndList(restrictions)) - tab.proj()``.
  Here ``target`` is the table to be populated, usually ``tab`` itself.
- ``suppress_errors`` - If ``True``, encountering an error will cancel the current ``make`` call, log the error, and continue to the next ``make`` call.
  Error messages will be logged in the job reservation table (if ``reserve_jobs`` is ``True``) and returned as a list.
  See also ``return_exception_objects`` and ``reserve_jobs``.
  Defaults to ``False``.
- ``return_exception_objects`` - If ``True``, error objects are returned instead of error messages.
  This applies only when ``suppress_errors`` is ``True``.
  Defaults to ``False``.
- ``reserve_jobs`` - If ``True``, reserves job to indicate to other distributed processes.
  The job reservation table may be access as ``schema.jobs``.
  Errors are logged in the jobs table.
  Defaults to ``False``.
- ``order`` - The order of execution, either ``"original"``, ``"reverse"``, or ``"random"``.
  Defaults to ``"original"``.
- ``display_progress`` - If ``True``, displays a progress bar.
  Defaults to ``False``.
- ``limit`` - If not ``None``, checks at most this number of keys.
  Defaults to ``None``.
- ``max_calls`` - If not ``None``, populates at most this many keys.
  Defaults to ``None``, which means no limit.
