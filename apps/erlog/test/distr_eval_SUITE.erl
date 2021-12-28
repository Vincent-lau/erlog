-module(distr_eval_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([tc_three_workers/1]).

-import(dl_repr, [cons_atom/2]).

all() ->
  [tc_three_workers].



init_per_testcase(tc_three_workers, Config) ->
  net_kernel:start([coor, shortnames]),
  ct:pal("config ~p~n", [Config]),
  NodePids =
    lists:map(fun(Num) ->
                 Cmd =
                   io_lib:format("erl -noshell -noinput -sname worker~w -pa ~s",
                                 [Num, "../../lib/erlog/ebin/"]),
                 ct:pal("current dir ~p, and cmd ~s~n", [file:get_cwd(), Cmd]),
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
  ProgName = ?config(data_dir, Config) ++ "../eval_SUITE_data/tc.dl",
  ct:pal(ProgName),
  coordinator:start_link(ProgName),
  timer:sleep(500),
  R = rpc:call(worker1@vincembp, worker, start, []),
  ct:pal("rpc call result ~p~n", [R]),
  rpc:call(worker2@vincembp, worker, start, []),
  rpc:call(worker3@vincembp, worker, start, []),

  FName = ?config(data_dir, Config) ++ "../tmp/final_db",
  Res = dbs:read_db(FName),
  ct:pal("Total result db is~n~s~n", [dbs:to_string(Res)]),
  ResQ = dbs:get_rel_by_pred("reachable", Res),
  FName2 = ?config(data_dir, Config) ++ "../eval_SUITE_data/reachable.csv",
  {ok, Stream} = file:open(FName2, [read]),
  Output = read_data(Stream),
  Ans = cons_db_from_data(Output, "reachable"),
  ct:pal("The result database is:~n~s~n and the ans db is ~n~s~n",
         [dbs:to_string(ResQ), dbs:to_string(Ans)]),
  true = dbs:equal(Ans, ResQ).

read_data(S) ->
  case io:get_line(S, '') of
    eof ->
      [];
    Line when is_list(Line) ->
      {ok, Tokens, _} = erl_scan:string(Line),
      [lists:map(fun({_, _, Args}) -> Args end, Tokens) | read_data(S)]
  end.

cons_db_from_data(Data, AtomName) ->
  dbs:from_list(
    lists:map(fun(Args) -> cons_atom(AtomName, Args) end, Data)).
