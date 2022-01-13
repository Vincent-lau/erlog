-module(timing).

-compile(export_all).

-define(PROG, "apps/erlog/examples/tc.dl").

-define(tmp_path, "apps/erlog/test/tmp/").


ave_time(single, N) ->
  Times = [time_single_node() || _ <- lists:seq(1, N)],
  lists:sum(Times) / N / 1000.

ave_time(distr, Repeats, NumWorkers, NumTasks) ->
  Times = [time_distr_nodes(NumWorkers, NumTasks) || _ <- lists:seq(1, Repeats)],
  lists:sum(Times) / Repeats / 1000.

time_single_node() ->
  {Time, _R} =
    timer:tc(erlog_worker,
             run_program,
             [single, "apps/erlog/examples/tc-large.dl", "reachable"]),
  io:format("time used in millisecond is ~p~n", [Time / 1000]),
  Time.

time_distr_nodes(NumWorkers, NumTasks) ->
  Cfg = erlog_worker:distr_setup("apps/erlog/examples/tc-large.dl", NumWorkers, NumTasks,
                                ?tmp_path),
  io:format("set up complete~n"),
  {Time, _R} = timer:tc(erlog_worker, distr_run, [Cfg, "reachable", ?tmp_path]),
  io:format("ready to clean up~n"),
  erlog_worker:distr_clean(Cfg),
  io:format("time used in millisecond is ~p~n", [Time / 1000]),
  Time.
