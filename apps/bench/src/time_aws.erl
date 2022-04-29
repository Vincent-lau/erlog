-module(time_aws).

-compile(export_all).

-define(PROG, "apps/bench/bench_program/tc_bench.dl").

coor_node() ->
  'coor@34.251.15.148'.

worker_nodes() ->
  ['worker1@54.216.16.41', 'worker2@34.244.44.196', 'worker3@34.240.39.149', 'worker4@54.229.87.41'].


start() ->
  R2 = erpc:multicall(worker_nodes(), worker, start_working, []),
  io:format("worker start_working rpc results ~p~n", [R2]),
  Time = timer:tc(coordinator, wait_for_finish, [1000 * 60]),
  io:format("time is ~p~n", [Time]).
  % Res = dbs:read_db(?TMP_DIR ++ "final_db"),
  % ResQL = lists:map(fun(QryName) -> dbs:get_rel_by_name(QryName, Res) end, QryNames).
  % io:format("ResQL ~n~s~n", [lists:map(fun dbs:to_string/1, ResQL)]).
