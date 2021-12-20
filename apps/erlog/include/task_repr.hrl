-type task_state() :: idle | in_progress | finished.
-type task_category() :: evaluate | wait | terminate.

-record(task,
        {task_num :: integer(),
         stage_num :: integer(),
         state :: task_state(),
         type :: task_category()}).

-type mr_task() ::
  #task{task_num :: integer(),
         stage_num :: integer(),
         state :: task_state(),
         type :: task_category()}.
