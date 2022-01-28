-module(tasks).

-export([set_finished/1, set_idle/1, set_in_prog/1, reset_time/1]).
-export([is_idle/1, is_finished/1, equals/2]).
-export([new_task/0, new_task/2, new_task/4, new_wait_task/0, new_terminate_task/0]).

-include("../include/task_repr.hrl").

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

-spec set_finished(mr_task()) -> mr_task().
set_finished(T = #task{}) ->
  T#task{state = finished}.

-spec set_idle(mr_task()) -> mr_task().
set_idle(T = #task{}) ->
  T#task{state = idle}.

-spec set_in_prog(mr_task()) -> mr_task().
set_in_prog(T = #task{}) ->
  T#task{state = in_progress}.

-spec reset_time(mr_task()) -> mr_task().
reset_time(T = #task{}) ->
  T#task{start_time = erlang:system_time(seconds)}.

%% @doc generate a dummy task
-spec new_task() -> mr_task().
new_task() ->
  new_task(0, 0, idle, evaluate).

-spec new_wait_task() -> mr_task().
new_wait_task() ->
  T = new_task(),
  T#task{type = wait}.

-spec new_terminate_task() -> mr_task().
new_terminate_task() ->
  T = new_task(),
  T#task{type = terminate}.

%% @doc generate a new eval idle task
-spec new_task(integer(), integer()) -> mr_task().
new_task(StageNum, TaskNum) ->
  new_task(StageNum, TaskNum, idle, evaluate).

-spec new_task(integer(), integer(), task_state(), task_category()) -> mr_task().
new_task(StageNum, TaskNum, TaskState, TaskType) ->
  #task{task_num = TaskNum,
        stage_num = StageNum,
        state = TaskState,
        type = TaskType,
        start_time = erlang:system_time(seconds)}.
