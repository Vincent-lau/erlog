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
  ok = wait_for_finish(),
  FinalDB = coordinator:collect_results(1, "apps/erlog/test/tmp/"),
  FinalDBQ = dbs:get_rel_by_pred(QryName, FinalDB),
  io:format("distributed evaluation, result db is ~n~s~n", [dbs:to_string(FinalDBQ)]).

run_program(distr, ProgName, QryName, NumWorkers) ->
  Cfg = distr_setup(ProgName, NumWorkers),
  distr_run(Cfg, QryName),
  distr_clean(Cfg).

spin_checker() ->
  case coordinator:done() of
    true ->
      io:format("The coordinator has finished its job.~n"),
      exit(done);
    false ->
      timer:sleep(100),
      io:format("still waiting, currently the coordinator is in stage ~p of "
                "execution~n",
                [coordinator:get_current_stage_num()]),
      spin_checker()
  end.

wait_for_finish() ->
  {Pid, Ref} = spawn_monitor(fun () -> spin_checker() end),
  receive
    {'DOWN', Ref, process, Pid, done} ->
      ok
  after 30000 ->
    exit(Pid, timeout),
    timeout
  end.
