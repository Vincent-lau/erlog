-module(distr_eval_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0, init_per_group/2, end_per_group/2, init_per_testcase/2,
         end_per_testcase/2, init_per_suite/1, end_per_suite/1]).
-export([tc3workers/1, tc4workers/1, tc6workers/1, tclarge4workers/1, scc4workers/1,
         rsg4workers/1, marrying4workers/1, nonlinear4workers/1]).

-import(dl_repr, [cons_atom/2]).

all() ->
  [tc4workers].

groups() ->
  [{tc_many_workers, [], [tc4workers, tc3workers, tc6workers]}].

init_per_suite(Config) ->
  application:start(erlog),
  net_kernel:start([coor, shortnames]),
  ProgramDir = ?config(data_dir, Config) ++ "../example_program/",
  [{program_dir, ProgramDir} | Config].

end_per_suite(_Config) ->
  net_kernel:stop(),
  application:stop(erlog).

init_per_testcase(nonlinear4workers, Config) ->
  multi_worker_init(30, 4, "non-linear-ancestor.dl", Config);
init_per_testcase(marrying4workers, Config) ->
  multi_worker_init(26, 4, "marrying-a-widower.dl", Config);
init_per_testcase(rsg4workers, Config) ->
  multi_worker_init(22, 4, "rsg.dl", Config);
init_per_testcase(scc4workers, Config) ->
  multi_worker_init(18, 4, "scc.dl", Config);
init_per_testcase(tclarge4workers, Config) ->
  multi_worker_init(14, 4, "tc-large.dl", Config);
init_per_testcase(tc3workers, Config) ->
  multi_worker_init(1, 3, "tc.dl", Config);
init_per_testcase(tc4workers, Config) ->
  multi_worker_init(4, 4, "tc.dl", Config);
init_per_testcase(tc6workers, Config) ->
  multi_worker_init(8, 6, "tc.dl", Config).

end_per_testcase(nonlinear4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(marrying4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(rsg4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(scc4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tclarge4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tc3workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tc4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tc6workers, Config) ->
  multi_worker_stop(Config).

init_per_group(tc_many_workers, Config) ->
  [{query_name, "reachable"} | Config].

end_per_group(tc_many_workers, _Config) ->
  ok.

nonlinear4workers(Config) ->
  dist_eval_tests(Config, "non-linear-ancestor.dl", "ancestor").

marrying4workers(Config) ->
  dist_eval_tests(Config, "marryin-a-widower.dl", "grandfather").

rsg4workers(Config) ->
  dist_eval_tests(Config, "rsg.dl", "rsg").

tclarge4workers(Config) ->
  dist_eval_tests(Config, "tc-large.dl", "tc_large").

tc3workers(Config) ->
  dist_eval_tests(Config, "tc.dl", "reachable").

tc4workers(Config) ->
  dist_eval_tests(Config, "tc.dl", "reachable").

tc6workers(Config) ->
  dist_eval_tests(Config, "tc.dl", "reachable").

scc4workers(Config) ->
  dist_eval_tests(Config, "scc.dl", "scc").

multi_worker_init(InitialNum, NumWorkers, ProgName, Config) ->
  TmpDir = get_tmp_dir(ProgName, NumWorkers, Config),
  clean_tmp(TmpDir),
  {ok, Pid} = coordinator:start_link(?config(program_dir, Config) ++ ProgName, TmpDir),
  {NodePids, NodeNames} = start_workers(InitialNum, NumWorkers),
  [{prog_name, ProgName},
   {worker_pids, NodePids},
   {worker_names, NodeNames},
   {num_workers, NumWorkers},
   {tmp_dir, TmpDir},
   {coor_pid, Pid}
   | Config].

multi_worker_stop(Config) ->
  stop_workers(Config),
  coordinator:stop().

get_tmp_dir(ProgName, NumWorkers, Config) ->
  BaseName = filename:basename(ProgName, ".dl"),
  PrivDir = ?config(priv_dir, Config),
  % tmp-progname-#workers e.g. ...log_private/tmp-tc-3/
  PrivDir ++ "tmp-" ++ BaseName ++ "-" ++ integer_to_list(NumWorkers) ++ "/".

clean_tmp(TmpPath) ->
  case filelib:is_dir(TmpPath) of
    true ->
      file:del_dir_r(TmpPath);
    false ->
      ok
  end,
  ok = file:make_dir(TmpPath).

start_workers(InitialNum, NumWorkers) ->
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
  ct:pal("node port numbers that are started~p~n", [NodePids]),
  timer:sleep(2000),
  R1 = erpc:multicall(NodeNames, worker, start, []),
  ct:pal("rpc calls to start workers result ~p~n", [R1]),
  {NodePids, NodeNames}.

stop_workers(Config) ->
  NodePids = ?config(worker_pids, Config),
  NodeNames = ?config(worker_names, Config),
  TmpL = erpc:multicall(NodeNames, worker, stop, []),
  ct:pal("results of calling worker stop ~p~n", [TmpL]),
  TmpM = lists:map(fun(Pid) -> erlang:port_close(Pid) end, NodePids),
  ct:pal("results of calling port close ~p~n", [TmpM]).

dist_eval_tests(Config, ProgName, QryName) ->
  % open file and read program
  NodeNames = ?config(worker_names, Config),
  R2 = erpc:multicall(NodeNames, worker, start_working, []),
  ct:pal("rpc call to start_working ~p", [R2]),

  ok = wait_for_finish(),
  Res = dbs:read_db(?config(tmp_dir, Config) ++ "final_db"),
  ct:pal("Total result db is~n~s~n", [dbs:to_string(Res)]),
  ResQ = dbs:get_rel_by_pred(QryName, Res),
  ct:pal("getting the query of ~s the result db: ~n~s~n", [QryName, dbs:to_string(Res)]),
  Ans = dbs:read_db(?config(program_dir, Config) ++ QryName ++ ".csv", QryName),
  ct:pal("Ans db is ~n~s~n", [dbs:to_string(Ans)]),
  true = dbs:equal(Ans, ResQ).

wait_for_finish() ->
  register(tester, self()),
  spawn(fun Waiter() ->
              case coordinator:done() of
                true ->
                  ct:pal("The coordinator has finished its job.~n"),
                  tester ! done;
                false ->
                  timer:sleep(500),
                  ct:pal("still waiting, current the coordinator is in stage ~p of execution~n",
                         [coordinator:get_current_stage_num()]),
                  Waiter()
              end
        end),
  receive
    done ->
      ok
  after 30000 ->
    timeout
  end.
