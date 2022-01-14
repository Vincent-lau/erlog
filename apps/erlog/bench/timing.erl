-module(timing).

-compile(export_all).

-define(PROG, "apps/erlog/examples/tc-large.dl").
-define(tmp_path, "apps/erlog/test/tmp/").

ave_time(single, N) ->
  Times = [time_single_node() || _ <- lists:seq(1, N)],
  lists:sum(Times) / N / 1000.

write_results(Res, Repeats) ->
  {ok, Stream} = file:open("apps/erlog/bench/timing_res.txt", [write]),
  io:format(Stream,
            "results of timing measurement: {~p*times, #workers} ~n~p~n",
            [Repeats, Res]),
  file:close(Stream).

time_against_workers(MaxWorkers) ->
  time_against_workers(1, MaxWorkers, 10, []).

time_against_workers(NumWorkers, MaxWorkers, Repeats, Acc) when NumWorkers > MaxWorkers ->
  Res =
    lists:zip(
      lists:reverse(Acc), lists:seq(1, MaxWorkers, 2)),
  io:format("final time against #workers ~p~n", [Res]),
  write_results(Res, Repeats),
  Res;
time_against_workers(NumWorkers, MaxWorkers, Repeats, Acc)
  when NumWorkers =< MaxWorkers ->
  io:format("currently ~p number of workers~n", [NumWorkers]),
  Time = all_times(distr, Repeats, NumWorkers, MaxWorkers + 4),
  io:format("times for ~p runs in this round is ~p~n", [Repeats, Time]),
  time_against_workers(NumWorkers + 2, MaxWorkers, Repeats, [Time | Acc]).

all_times(distr, Repeats, NumWorkers, NumTasks) ->
  [time_distr_nodes(NumWorkers, NumTasks) || _ <- lists:seq(1, Repeats)].

time_single_node() ->
  {Time, _R} =
    timer:tc(erlog_worker,
             run_program,
             [single, ?PROG, "reachable"]),
  io:format("time used in millisecond is ~p~n", [Time / 1000]),
  Time.

time_distr_nodes(NumWorkers, NumTasks) ->
  Cfg =
    erlog_worker:distr_setup(?PROG,
                             NumWorkers,
                             NumTasks,
                             ?tmp_path),
  io:format("set up complete~n"),
  {Time, _R} = timer:tc(erlog_worker, distr_run, [Cfg, "reachable", ?tmp_path]),
  io:format("ready to clean up~n"),
  erlog_worker:distr_clean(Cfg),
  io:format("time used in millisecond is ~p~n", [Time / 1000]),
  Time.
