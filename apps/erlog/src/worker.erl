-module(worker).

-compile(export_all).

-include("../include/task_repr.hrl").
-include("../include/coor_params.hrl").

-include_lib("kernel/include/logger.hrl").

-import(dbs, [db_to_string/1]).

-define(coor_node, coor@vincembp).

start() ->
  net_kernel:start([worker1, shortnames]),
  true = net_kernel:connect_node(?coor_node),
  global:sync(), % make sure that worker sees the registered name
  Pid = global:whereis_name(coor),
  Prog = rpc:call(?coor_node, coordinator, get_prog, [Pid]),
  StaticDB = rpc:call(?coor_node, coordinator, get_static_db, [Pid]),
  NumTasks = rpc:call(?coor_node, coordinator, get_num_tasks, [Pid]),
  work(Pid, Prog, StaticDB, NumTasks).

work(Pid, Prog, StaticDB, NumTasks)->
  case rpc:call(?coor_node, coordinator, assign_task, [Pid]) of
    T=#task{type = evaluate, task_num = TaskNum, stage_num = StageNum} ->
      FileName = io_lib:format("~stask-~w-~w", [?inter_dir, StageNum, TaskNum]),
      ?LOG_DEBUG(#{read_from_file => FileName}),
      {ok, Stream}  = file:open(FileName, [read]),
      {F, _R} = preproc:lex_and_parse(Stream),
      file:close(Stream),
      Facts = dbs:from_list(F),
      ?LOG_DEBUG(#{facts => db_to_string(Facts), rules => Prog}),
      NewDB = eval:imm_conseq(Prog, Facts),
      ?LOG_DEBUG(#{new_db => db_to_string(NewDB)}),
      DeltaDB = dbs:diff(NewDB, Facts),
      % hash the new DB locally and write to disk
      % with only tuples that have not been generated before
      frag:hash_frag(DeltaDB, Prog, NumTasks, StageNum + 1, ?inter_dir),
      % call finish task on coordinator
      rpc:cast(?coor_node, coordinator, finish_task, [Pid, T]),
      % request new tasks 
      io:format("stage-~w task-~w finished, requesting new task~n", [StageNum, TaskNum]),
      work(Pid, Prog, StaticDB, NumTasks);
    #task{type = wait} ->
      io:format("this is a wait task, sleeping for 3 sec~n"),
      timer:sleep(3000);
    {badrpc, Reason} -> % TODO do I need to distinguish different errors
      io:format("getting task from coordinator failed due to ~p~n", [Reason])
  end.
