-module(distr_eval_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0, init_per_group/2, end_per_group/2]).
-export([tc3workers/1, tc4workers/1, tc6workers/1]).

-import(dl_repr, [cons_atom/2]).

all() ->
  [{group, tc_many_workers}].

groups() ->
  [{tc_many_workers, [], [tc4workers, tc3workers, tc6workers]}].


init_per_group(tc_many_workers, Config) ->
  net_kernel:start([coor, shortnames]),
  Config.

end_per_group(tc_many_workers, _Config) ->
  net_kernel:stop().

tc3workers(Config) ->
  C = start_workers(1, 3, Config),
  C2 = config_tmp_dir("tc.dl", 3, C),
  clean_tmp(C2),
  ct:pal("config for 3 workers~p~n", [Config]),
  dist_eval_tests(C2, "tc.dl", "reachable").

tc4workers(Config) ->
  C = start_workers(4, 4, Config),
  C2 = config_tmp_dir("tc.dl", 4, C),
  clean_tmp(C2),
  ct:pal("config for 4 workers~p~n", [Config]),
  dist_eval_tests(C2, "tc.dl", "reachable").

tc6workers(Config) ->
  C = start_workers(8, 6, Config),
  C2 = config_tmp_dir("tc.dl", 6, C),
  clean_tmp(C2),
  dist_eval_tests(C2, "tc.dl", "reachable").

config_tmp_dir(ProgName, NumWorkers, Config) ->
  BaseName = filename:basename(ProgName, ".dl"),
  PrivDir = ?config(priv_dir, Config),
  % tmp-progname-#workers e.g. ...log_private/tmp-tc-3/
  TmpDir = PrivDir ++ "tmp-" ++ BaseName ++ "-" ++ integer_to_list(NumWorkers) ++ "/",
  [{tmp_dir, TmpDir} | Config].

clean_tmp(Config) ->
  TmpPath = ?config(tmp_dir, Config),
  case filelib:is_dir(TmpPath) of
    true ->
      file:del_dir_r(TmpPath);
    false ->
      ok
  end,
  ok = file:make_dir(TmpPath).

start_workers(InitialNum, NumWorkers, Config) ->
  NodePids =
    lists:map(fun(Num) ->
                 Cmd =
                   io_lib:format("erl -noshell -noinput -sname worker~w -pa ~s",
                                 [Num, "../../lib/erlog/ebin/"]),
                 ct:pal("starting worker #~w~n", [Num]),
                 erlang:open_port({spawn, Cmd}, [])
              end,
              lists:seq(InitialNum, InitialNum + NumWorkers - 1)),
  NodeNames =
    [list_to_atom("worker" ++ integer_to_list(N) ++ "@vincembp")
     || N <- lists:seq(InitialNum, InitialNum + NumWorkers - 1)],
  [{worker_pids, NodePids}, {worker_names, NodeNames}, {num_workers, NumWorkers} | Config].

stop_workers(Config) ->
  NodePids = ?config(worker_pids, Config),
  NodeNames = ?config(worker_names, Config),
  TmpL = lists:map(fun(Name) -> rpc:call(Name, worker, stop, []) end, NodeNames),
  ct:pal("results of calling worker stop ~p~n", [TmpL]),
  TmpM = lists:map(fun(Pid) -> erlang:port_close(Pid) end, NodePids),
  ct:pal("results of calling port close ~p~n", [TmpM]).

dist_eval_tests(Config, ProgName, QryName) ->
  % open file and read program
  {ok, Pid} =
    coordinator:start_link(?config(data_dir, Config) ++ ProgName, ?config(tmp_dir, Config)),

  timer:sleep(500),
  NodeNames = ?config(worker_names, Config),
  R = lists:map(fun(Name) -> rpc:call(Name, worker, start, []) end, NodeNames),
  ct:pal("rpc call result ~p~n", [R]),

  ok = wait_for_finish(Pid),
  Res = dbs:read_db(?config(tmp_dir, Config) ++ "final_db"),
  ct:pal("Total result db is~n~s~n", [dbs:to_string(Res)]),
  ResQ = dbs:get_rel_by_pred("reachable", Res),
  Ans = dbs:read_db(?config(data_dir, Config) ++ QryName ++ ".csv", QryName),
  ct:pal("The result database is:~n~s~n and the ans db is ~n~s~n",
         [dbs:to_string(ResQ), dbs:to_string(Ans)]),
  true = dbs:equal(Ans, ResQ),
  % we want to stop workers and coordinators as part of the test to avoid conflicts
  % between different tests
  stop_workers(Config),
  coordinator:stop_coordinator(Pid).

wait_for_finish(Pid) ->
  register(tester, self()),
  spawn(fun Waiter() ->
              case coordinator:done(Pid) of
                true ->
                  ct:pal("The coordinator has finished its job.~n"),
                  tester ! done;
                false ->
                  timer:sleep(300),
                  Waiter()
              end
        end),
  receive
    done ->
      ok 
  after 100000 ->
    timeout
  end.
