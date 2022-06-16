-module(tasks).

-export([set_finished/1, set_idle/1, set_in_prog/1, set_worker/2, reset_time/1,
         set_size/2]).
-export([is_idle/1, is_finished/1, is_in_progress/1, is_eval/1, is_assigned/1, equals/2]).
-export([new_dummy_task/0, new_task/5, new_task/6, new_task/7, new_wait_task/0,
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
             stage_num = SN1},
       #task{task_num = TN2,
             stage_num = SN2}) ->
  TN1 == TN2 andalso SN1 == SN2.


-spec get_task_state(mr_task()) -> idle | in_progress | finished.
get_task_state(#task{statem = StateM}) ->
  element(1, sys:get_state(StateM)).

-spec is_idle(mr_task()) -> boolean().
is_idle(T = #task{}) ->
  get_task_state(T) =:= idle.


-spec is_finished(mr_task()) -> boolean().
is_finished(T = #task{}) ->
  get_task_state(T) =:= finished.

-spec is_in_progress(mr_task()) -> boolean().
is_in_progress(T = #task{}) ->
  get_task_state(T) =:= in_progress.

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

-spec set_finished(mr_task()) -> ok.
set_finished(#task{statem = StateM}) ->
  ok = task_coor:finish_task(StateM).

-spec set_idle(mr_task()) -> ok.
set_idle(#task{statem = StateM}) ->
  ok = task_coor:reset_task(StateM).

-spec set_in_prog(mr_task()) -> ok.
set_in_prog(#task{statem = StateM}) ->
  ok = task_coor:request_task(StateM).

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
  new_task([], 0, 0, 0, 0, evaluate, 0).

-spec new_wait_task() -> mr_task().
new_wait_task() ->
  T = new_dummy_task(),
  T#task{type = wait}.

-spec new_terminate_task() -> mr_task().
new_terminate_task() ->
  T = new_dummy_task(),
  T#task{type = terminate}.

%% @doc generate a new eval idle task
-spec new_task(dl_program(), integer(), integer(), integer(), pid()) -> mr_task().
new_task(Program, ProgNum, StageNum, TaskNum, TaskSup) ->
  new_task(Program, ProgNum, StageNum, TaskNum, TaskSup, evaluate, 0).

-spec new_task(dl_program(),
               integer(),
               integer(),
               integer(),
               pid(),
               integer() | file:filename()) ->
                mr_task().
new_task(Program, ProgNum, StageNum, TaskNum, TaskSup, Size) when is_integer(Size) ->
  new_task(Program, ProgNum, StageNum, TaskNum, TaskSup, evaluate, Size);
new_task(Program, ProgNum, StageNum, TaskNum, TaskSup, TaskPath) when is_list(TaskPath) ->
  new_task(Program, ProgNum, StageNum, TaskNum, TaskSup, evaluate, TaskPath).

-spec new_task(dl_program(),
               integer(),
               integer(),
               integer(),
               pid(),
               task_category(),
               number() | file:filename()) ->
                mr_task().
new_task(Program, ProgNum, StageNum, TaskNum, 0, TaskType, Size) ->
  % dummy task, no need for state machine
  #task{prog = Program,
        prog_num = ProgNum,
        task_num = TaskNum,
        stage_num = StageNum,
        type = TaskType,
        start_time = erlang:monotonic_time(millisecond),
        assigned_worker = none,
        size = Size};
new_task(Program, ProgNum, StageNum, TaskNum, TaskSup, TaskType, Size)
  when is_number(Size) ->
  #task{prog = Program,
        prog_num = ProgNum,
        task_num = TaskNum,
        stage_num = StageNum,
        statem = start_task_statem(TaskSup),
        type = TaskType,
        start_time = erlang:monotonic_time(millisecond),
        assigned_worker = none,
        size = Size};
new_task(Program, ProgNum, StageNum, TaskNum, TaskSup, TaskType, TaskPath)
  when is_list(TaskPath) ->
  #task{prog = Program,
        prog_num = ProgNum,
        task_num = TaskNum,
        stage_num = StageNum,
        statem = start_task_statem(TaskSup),
        type = TaskType,
        start_time = erlang:monotonic_time(millisecond),
        assigned_worker = none,
        size = find_task_size(StageNum, TaskNum, TaskPath)}.


-spec start_task_statem(pid()) -> pid().
start_task_statem(Sup) ->
  pass.
