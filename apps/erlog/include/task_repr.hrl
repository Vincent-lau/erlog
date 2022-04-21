-include("data_repr.hrl").

-type task_state() :: idle | in_progress | finished.
-type task_category() :: evaluate | wait | terminate.

-record(task,
        {task_num :: integer(),
         stage_num :: integer(),
         prog :: dl_program(),
         prog_num :: integer(),
         delta_db :: dl_db_instance(),
         state :: task_state(),
         type :: task_category(),
         start_time :: integer(),
         assigned_worker :: node(),
         size :: number()}).

-type mr_task() :: #task{}.
