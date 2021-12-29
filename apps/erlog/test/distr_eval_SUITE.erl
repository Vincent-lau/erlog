-module(distr_eval_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([tc_three_workers/1]).

-import(dl_repr, [cons_atom/2]).

all() ->
  [tc_three_workers].

init_per_testcase(tc_three_workers, Config) ->
  clean_tmp(Config),
  net_kernel:start([coor, shortnames]),
  NodePids =
    lists:map(fun(Num) ->
                 Cmd =
                   io_lib:format("erl -noshell -noinput -sname worker~w -pa ~s",
                                 [Num, "../../lib/erlog/ebin/"]),
                 ct:pal("starting worker #~w~n", [Num]),
                 erlang:open_port({spawn, Cmd}, [{packet, 2}])
              end,
              lists:seq(1, 3)),
  [{worker_pids, NodePids} | Config].

end_per_testcase(tc_three_workers, Config) ->
  NodePids = ?config(worker_pids, Config),
  NodeNames =
    [list_to_atom("worker" ++ integer_to_list(N) ++ "@vincembp") || N <- lists:seq(1, 3)],
  TmpL = lists:map(fun(Name) -> rpc:call(Name, worker, stop, []) end, NodeNames),
  ct:pal("results of calling worker stop ~p~n", [TmpL]),
  TmpM = lists:map(fun(Pid) -> erlang:port_close(Pid) end, NodePids),
  ct:pal("results of calling port close ~p~n", [TmpM]),
  net_kernel:stop().

tc_three_workers(Config) ->
  dist_eval_tests(Config, "tc.dl", "reachable").

clean_tmp(Config) ->
  TmpPath = ?config(priv_dir, Config) ++ "tmp/",
  case filelib:is_dir(TmpPath) of
    true ->
      ok;
    false ->
      file:del_dir_r(TmpPath)
  end,
  ok = file:make_dir(TmpPath).

dist_eval_tests(Config, ProgName, QryName) ->
  % open file and read program
  {ok, _Pid} =
    coordinator:start_link(?config(data_dir, Config) ++ ProgName,
                           ?config(priv_dir, Config) ++ "tmp/"),

  timer:sleep(500),
  R = rpc:call(worker1@vincembp, worker, start, []),
  ct:pal("rpc call result ~p~n", [R]),
  rpc:call(worker2@vincembp, worker, start, []),
  rpc:call(worker3@vincembp, worker, start, []),

  Res = dbs:read_db(?config(data_dir, Config) ++ "../tmp/final_db"),
  ct:pal("Total result db is~n~s~n", [dbs:to_string(Res)]),
  ResQ = dbs:get_rel_by_pred("reachable", Res),
  Ans = dbs:read_db(?config(data_dir, Config) ++ QryName ++ ".csv", QryName),
  ct:pal("The result database is:~n~s~n and the ans db is ~n~s~n",
         [dbs:to_string(ResQ), dbs:to_string(Ans)]),
  true = dbs:equal(Ans, ResQ).
