-module(timing).

-compile(export_all).

-define(PROG, "apps/erlog/examples/tc.dl").

time_single_node() ->
  {Time, ok} = timer:tc(erlog_worker, run_program, [single, "apps/erlog/examples/tc-large.dl", "reachable"]),
  io:format("time used in millisecond is ~p~n", [Time / 1000]).

time_distr_nodes() ->
  Cfg = erlog_worker:distr_setup("apps/erlog/examples/tc-large.dl", 5),
  io:format("set up complete~n"),
  {Time, ok} = timer:tc(erlog_worker, distr_run, [Cfg, "reachable"]),
  io:format("ready to clean up~n"),
  erlog_worker:distr_clean(Cfg),
  io:format("time used in millisecond is ~p~n", [Time / 1000]).
