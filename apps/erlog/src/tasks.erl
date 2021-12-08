-module(tasks).

-compile(export_all).

-include("../include/task_repr.hrl").

-spec is_idle(mr_task()) -> boolean().
is_idle(#task{state = S}) when S =:= idle ->
  true;
is_idle(#task{}) ->
  false.

set_finished(T = #task{}) ->
  T#task{state = finished}.

set_idle(T = #task{}) ->
  T#task{state = idle}.

set_in_prog(T = #task{}) ->
  T#task{state = in_progress}.

%% @doc generate a dummy task
new_task() ->
  #task{task_num = 0,
        stage_num = 0,
        state = idle,
        type = evaluate}.

new_wait_task() ->
  T = new_task(),
  T#task{type = wait}.

-spec new_task(integer(), integer(), task_state(), task_category()) -> mr_task().
new_task(TaskNum, StageNum, TaskState, TaskType) ->
  #task{task_num = TaskNum,
        stage_num = StageNum,
        state = TaskState,
        type = TaskType}.

new_task(TaskNum, StageNum) ->
  #task{task_num = TaskNum,
        stage_num = StageNum,
        state = idle,
        type = evaluate}.
