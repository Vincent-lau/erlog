-module(worker).

-compile(export_all).

-include("../include/task_repr.hrl").

-define(COORDINATOR_NAME, coor@vincembp).

start() ->
  net_kernel:start([worker1, shortnames]),
  true = net_kernel:connect_node(?COORDINATOR_NAME),
  global:sync(), % make sure that worker sees the registered name
  Pid = global:whereis_name(coor),
  work(Pid, 1).

work(Pid, N) when N < 5 ->
  AssignedTask = rpc:call(?COORDINATOR_NAME, coordinator, assign_task, [Pid]),
  io:format("assigned task is ~p~n", [AssignedTask]),
  work(Pid, N + 1);
work(_, _) -> ok.

test() ->
  io:format("registered test ~p~n", [global:registered_names()]).