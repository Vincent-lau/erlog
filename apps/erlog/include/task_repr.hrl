-type task_state() :: idle | in_progress | finished.
-type task_category() :: evaluate | wait.

-record(task, {num :: integer(), state :: task_state(), type :: task_category()}).
