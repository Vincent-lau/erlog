-include("data_repr.hrl").

-type task_category() :: evaluate | wait | terminate.

-record(task,
        {task_num :: integer(),
         stage_num :: integer(),
         prog :: dl_program(),
         prog_num :: integer(),
         statem :: pid() | undefined,
         type :: task_category(),
         start_time :: integer(),
         assigned_worker :: node(),
         size :: number()}).

-type mr_task() :: #task{}.
