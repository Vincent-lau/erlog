%%%-------------------------------------------------------------------
%% @doc erlog single node worker
%% @end
%%%-------------------------------------------------------------------

-module(erlog_worker).

-include_lib("kernel/include/logger.hrl").

-export([run_program/3, run_program/4, distr_clean/1, distr_setup/2, distr_run/2]).

-spec run_program(Mode, file:filename(), string()) -> ok when Mode :: single | distr.
run_program(single, ProgName, QryName) ->
  {Facts, Rules} = preproc:lex_and_parse(file, ProgName),
  % preprocess rules
  Prog2 = preproc:process_rules(Rules),
  ?LOG_DEBUG("Input program is:~n~s~n", [dl_repr:program_to_string(Prog2)]),

  ?LOG_DEBUG("input data is ~p~n", [Facts]),
  % create EDB from input relations
  EDB = dbs:from_list(Facts),
  Res = eval:eval_all(Prog2, EDB),
  ResQ = dbs:get_rel_by_pred(QryName, Res),
  io:format("single node evaluation, result db is~n~s~n", [dbs:to_string(ResQ)]);
run_program(distr, ProgName, QryName) ->
  run_program(distr, ProgName, QryName, 4).

distr_setup(ProgName, NumWorkers) ->
  net_kernel:start(['coor@127.0.0.1', longnames]),
  coordinator:start_link(ProgName),
  Cfg = dconfig:start_cluster([worker], NumWorkers),
  dconfig:all_start(Cfg),
  Cfg.

distr_clean(Cfg) ->
  dconfig:stop_cluster(Cfg),
  coordinator:stop(),
  net_kernel:stop().

distr_run(Cfg, QryName) ->
  dconfig:all_work(Cfg),
  ok = coordinator:wait_for_finish(5000, 100),
  FinalDB = coordinator:collect_results(1, "apps/erlog/test/tmp/"),
  FinalDBQ = dbs:get_rel_by_pred(QryName, FinalDB),
  io:format("distributed evaluation, result db is ~n~s~n", [dbs:to_string(FinalDBQ)]).

run_program(distr, ProgName, QryName, NumWorkers) ->
  Cfg = distr_setup(ProgName, NumWorkers),
  distr_run(Cfg, QryName),
  distr_clean(Cfg).


