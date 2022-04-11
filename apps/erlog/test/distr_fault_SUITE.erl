%%% @doc
%%% This module tests the correctness of the engine when <em>workers</em> fail
-module(distr_fault_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0, init_per_group/2, end_per_group/2, init_per_testcase/2,
         end_per_testcase/2, init_per_suite/1, end_per_suite/1]).
-export([tc3workers/1, tc4workers/1, tc6workers/1, tclarge10workers/1, tc2_4workers/1,
         scc4workers/1, rsg4workers/1, marrying4workers/1, nonlinear4workers/1,
         pointsto4workers/1]).

-import(dl_repr, [cons_atom/2]).

all() ->
  [{group, worker_straggle}, {group, worker_fail}].

groups() ->
  [{worker_fail, [{repeat, 3}, shuffle, sequence], all_tests()},
   {worker_straggle, [{repeat, 3}, shuffle], all_tests()}].

all_tests() ->
  [tc4workers,
   tclarge10workers,
   rsg4workers,
   nonlinear4workers,
   scc4workers,
   marrying4workers,
   tc2_4workers,
   pointsto4workers].


ets_owner() ->
  receive
    stop ->
      exit(normal);
    _Other ->
      ets_owner()
  end.

init_per_suite(Config) ->
  application:start(erlog),
  net_kernel:start(['coor@127.0.0.1', longnames]),
  Pid = spawn(fun ets_owner/0),
  TabId = ets:new(dl_atom_names, [named_table, public, {heir, Pid, []}]),
  ProgramDir = ?config(data_dir, Config) ++ "../test_program/",
  [{table, TabId}, {table_owner, Pid}, {program_dir, ProgramDir} | Config].

end_per_suite(Config) ->
  ?config(table_owner, Config) ! stop,
  net_kernel:stop(),
  application:stop(erlog).

init_per_group(worker_fail, Config) ->
  [{mode, failure} | Config];
init_per_group(worker_straggle, Config) ->
  [{mode, straggle} | Config].

end_per_group(worker_fail, _Config) ->
  ok;
end_per_group(worker_straggle, _Config) ->
  ok.

init_per_testcase(nonlinear4workers, Config) ->
  multi_worker_init(4, "non-linear-ancestor.dl", Config);
init_per_testcase(marrying4workers, Config) ->
  multi_worker_init(4, "marrying-a-widower.dl", Config);
init_per_testcase(rsg4workers, Config) ->
  multi_worker_init(4, "rsg.dl", Config);
init_per_testcase(scc4workers, Config) ->
  multi_worker_init(4, "scc.dl", Config);
init_per_testcase(tclarge10workers, Config) ->
  multi_worker_init(10, "tc-large.dl", Config);
init_per_testcase(tc2_4workers, Config) ->
  multi_worker_init(4, "tc2.dl", Config);
init_per_testcase(tc3workers, Config) ->
  multi_worker_init(3, "tc.dl", Config);
init_per_testcase(tc4workers, Config) ->
  multi_worker_init(4, "tc.dl", Config);
init_per_testcase(tc6workers, Config) ->
  multi_worker_init(6, "tc.dl", Config);
init_per_testcase(pointsto4workers, Config) ->
  multi_worker_init(4, "pointsto.dl", Config).

end_per_testcase(nonlinear4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(marrying4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(rsg4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(scc4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tclarge10workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tc2_4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tc3workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tc4workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(tc6workers, Config) ->
  multi_worker_stop(Config);
end_per_testcase(pointsto4workers, Config) ->
  multi_worker_stop(Config).

nonlinear4workers(Config) ->
  dist_eval_tests(Config, "ancestor").

marrying4workers(Config) ->
  dist_eval_tests(Config, "grandfather").

rsg4workers(Config) ->
  dist_eval_tests(Config, "rsg").

tclarge10workers(Config) ->
  dist_eval_tests(Config, "tc_large").

tc2_4workers(Config) ->
  dist_eval_tests(Config, "reachable").

tc3workers(Config) ->
  dist_eval_tests(Config, "reachable").

tc4workers(Config) ->
  dist_eval_tests(Config, "reachable").

tc6workers(Config) ->
  dist_eval_tests(Config, "reachable").

scc4workers(Config) ->
  dist_eval_tests(Config, "scc").

pointsto4workers(Config) ->
  dist_eval_tests(Config, ["alias", "assign"]).

multi_worker_init(NumWorkers, ProgName, Config) ->
  TmpDir = get_tmp_dir(ProgName, NumWorkers, Config),
  clean_tmp(TmpDir),
  ct:pal("program name ~p~n", [ProgName]),
  {ok, Pid} = coordinator:start_link(?config(program_dir, Config) ++ ProgName, TmpDir),
  Cfg =
    case ?config(mode, Config) of
      straggle ->
        slow_start_workers(NumWorkers);
      failure ->
        fail_start_workers(NumWorkers)
    end,
  [{prog_name, ProgName}, {worker_cfg, Cfg}, {tmp_dir, TmpDir}, {coor_pid, Pid} | Config].

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

fail_start_workers(NumWorkers) ->
  start_workers(NumWorkers, failure).

slow_start_workers(NumWorkers) ->
  start_workers(NumWorkers, straggle).

start_workers(NumWorkers, Mode) ->
  ct:pal("current path~p~n", [file:get_cwd()]),
  Cfg = dconfig:start_cluster([worker], NumWorkers, "../../lib/erlog/ebin"),
  ct:pal("result of starting workers ~p~n", [Cfg]),
  R = case Mode of
        failure ->
          dconfig:fail_start(Cfg, abnormal_worker:num_failures(NumWorkers));
        straggle ->
          dconfig:slow_start(Cfg, abnormal_worker:num_stragglers(NumWorkers))
      end,
  ct:pal("results of all_start ~p~n", [R]),
  Cfg.

stop_workers(Config) ->
  R = dconfig:stop_cluster(?config(worker_cfg, Config)),
  ct:pal("results of stopping workers ~p~n", [R]).

dist_eval_tests(Config, QryNames) when is_list(hd(QryNames)) ->
  WorkerCfg = ?config(worker_cfg, Config),
  dconfig:all_work(WorkerCfg),
  ok = coordinator:wait_for_finish(1000 * 120),
  Res = dbs:read_db(?config(tmp_dir, Config) ++ "final_db"),
  ct:pal("Total result db is~n~s~n", [dbs:to_string(Res)]),
  ResQL = lists:map(fun(QryName) -> dbs:get_rel_by_name(QryName, Res) end, QryNames),
  lists:foreach(fun({QryName, ResQ}) ->
                   ct:pal("getting the query of ~s the result db: ~n~s~n",
                          [QryName, dbs:to_string(ResQ)])
                end,
                lists:zip(QryNames, ResQL)),
  AnsL =
    lists:map(fun(QryName) ->
                 dbs:read_db(?config(program_dir, Config) ++ QryName ++ ".csv", QryName)
              end,
              QryNames),

  ct:pal("Ans db is ~n~s~n", [lists:map(fun dbs:to_string/1, AnsL)]),

  true = lists:all(fun({ResQ, Ans}) -> dbs:equal(Ans, ResQ) end, lists:zip(ResQL, AnsL));
dist_eval_tests(Config, QryName) ->
  dist_eval_tests(Config, [QryName]).
