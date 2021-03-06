-module(tasks).

-export([set_finished/1, set_idle/1, set_in_prog/1, set_worker/2, reset_time/1,
         set_size/2]).
-export([is_idle/1, is_finished/1, is_in_progress/1, is_eval/1, is_assigned/1, equals/2]).
-export([new_dummy_task/0, new_task/4, new_task/5, new_task/7, new_wait_task/0,
         new_terminate_task/0, reset_task/1]).
-export([get_start_time/1]).

-include("../include/task_repr.hrl").

-spec get_start_time(mr_task()) -> integer().
get_start_time(#task{start_time = ST}) ->
  ST.

%%----------------------------------------------------------------------
%% @doc
%% We identify a task by its task# and stage# and its state
%% @end
%%----------------------------------------------------------------------
-spec equals(mr_task(), mr_task()) -> boolean().
equals(#task{task_num = TN1,
             stage_num = SN1,
             state = S1},
       #task{task_num = TN2,
             stage_num = SN2,
             state = S2}) ->
  TN1 == TN2 andalso SN1 == SN2 andalso S1 =:= S2.

-spec is_idle(mr_task()) -> boolean().
is_idle(#task{state = S}) when S =:= idle ->
  true;
is_idle(#task{}) ->
  false.

-spec is_finished(mr_task()) -> boolean().
is_finished(#task{state = finished}) ->
  true;
is_finished(#task{}) ->
  false.

-spec is_in_progress(mr_task()) -> boolean().
is_in_progress(#task{state = in_progress}) ->
  true;
is_in_progress(#task{}) ->
  false.

-spec is_eval(mr_task()) -> boolean().
is_eval(#task{type = evaluate}) ->
  true;
is_eval(#task{}) ->
  false.

-spec is_assigned(mr_task()) -> boolean().
is_assigned(#task{assigned_worker = none}) ->
  false;
is_assigned(#task{}) ->
  true.

-spec set_finished(mr_task()) -> mr_task().
set_finished(T = #task{}) ->
  T#task{state = finished}.

-spec set_idle(mr_task()) -> mr_task().
set_idle(T = #task{}) ->
  T#task{state = idle}.

-spec set_in_prog(mr_task()) -> mr_task().
set_in_prog(T = #task{}) ->
  T#task{state = in_progress}.

set_worker(T = #task{}, WorkerNode) ->
  T#task{assigned_worker = WorkerNode}.

-spec find_task_size(integer(), integer(), file:filename()) -> number().
find_task_size(StageNum, TaskNum, FilePath) ->
  TaskFile = io_lib:format("~stask-~w-~w", [FilePath, StageNum, TaskNum]),
  filelib:file_size(TaskFile).

-spec set_size(mr_task(), number() | file:filename()) -> mr_task().
set_size(T = #task{}, Size) when is_float(Size) ->
  T#task{size = Size};
set_size(T = #task{task_num = TN, stage_num = SN}, FilePath) when is_list(FilePath) ->
  S = find_task_size(SN, TN, FilePath),
  T#task{size = S}.

-spec reset_time(mr_task()) -> mr_task().
reset_time(T = #task{}) ->
  T#task{start_time = erlang:monotonic_time(millisecond)}.

%% @doc this is to reset the state of the task when the assigned worker dies
-spec reset_task(mr_task()) -> mr_task().
reset_task(T = #task{}) ->
  set_worker(reset_time(set_idle(T)), none).

%% @doc generate a dummy task
-spec new_dummy_task() -> mr_task().
new_dummy_task() ->
  new_task([], 0, 0, 0, idle, evaluate, 0).

-spec new_wait_task() -> mr_task().
new_wait_task() ->
  T = new_dummy_task(),
  T#task{type = wait}.

-spec new_terminate_task() -> mr_task().
new_terminate_task() ->
  T = new_dummy_task(),
  T#task{type = terminate}.

%% @doc generate a new eval idle task
-spec new_task(dl_program(), integer(), integer(), integer()) -> mr_task().
new_task(Program, ProgNum, StageNum, TaskNum) ->
  new_task(Program, ProgNum, StageNum, TaskNum, idle, evaluate, 0).

-spec new_task(dl_program(),
               integer(),
               integer(),
               integer(),
               integer() | file:filename()) ->
                mr_task().
new_task(Program, ProgNum, StageNum, TaskNum, Size) when is_integer(Size) ->
  new_task(Program, ProgNum, StageNum, TaskNum, idle, evaluate, Size);
new_task(Program, ProgNum, StageNum, TaskNum, TaskPath) when is_list(TaskPath) ->
  new_task(Program, ProgNum, StageNum, TaskNum, idle, evaluate, TaskPath).

-spec new_task(dl_program(),
               integer(),
               integer(),
               integer(),
               task_state(),
               task_category(),
               number() | file:filename()) ->
                mr_task().
new_task(Program, ProgNum, StageNum, TaskNum, TaskState, TaskType, Size)
  when is_number(Size) ->
  #task{prog = Program,
        prog_num = ProgNum,
        task_num = TaskNum,
        stage_num = StageNum,
        state = TaskState,
        type = TaskType,
        start_time = erlang:monotonic_time(millisecond),
        assigned_worker = none,
        size = Size};
new_task(Program, ProgNum, StageNum, TaskNum, TaskState, TaskType, TaskPath)
  when is_list(TaskPath) ->
  #task{prog = Program,
        prog_num = ProgNum,
        task_num = TaskNum,
        stage_num = StageNum,
        state = TaskState,
        type = TaskType,
        start_time = erlang:monotonic_time(millisecond),
        assigned_worker = none,
        size = find_task_size(StageNum, TaskNum, TaskPath)}.
