-module(timing).

-compile(export_all).

-define(PROG, "apps/erlog/bench/bench_program/scc_bench.dl").
-define(tmp_path, "apps/erlog/test/tmp/").
-define(res_path, "apps/erlog/bench/results/timing_res.txt").

start() ->
  io:format("current timing program ~p~n", [?PROG]),
  time_against_workers(wallclock, 1, 5, 2, []).

start(single) ->
  repeat_times(single, foo, 3).

-spec time_against_workers(TimeType, integer()) -> list()
  when TimeType :: cpu | wallclock.
time_against_workers(TimeType, MaxWorkers) ->
  time_against_workers(TimeType, 1, MaxWorkers, 10, []).

%%----------------------------------------------------------------------
%% @doc
%% This function will measure the time taken for a particular number of workers
%% to compute a particular program.
%%
%% @param Acc number of trials
%%
%% @returns a list of {[time1, time2, ...], #workers}
%% @end
%%----------------------------------------------------------------------
-spec time_against_workers(TimeType, integer(), integer(), integer(), list()) -> list()
  when TimeType :: cpu | wallclock.
time_against_workers(_TimeType, NumWorkers, MaxWorkers, Repeats, Acc)
  when NumWorkers > MaxWorkers ->
  Res =
    lists:zip(
      lists:reverse(Acc), lists:seq(1, MaxWorkers, 2)),
  io:format("final time against #workers ~p~n", [Res]),
  write_results(Res, Repeats),
  Res;
time_against_workers(TimeType, NumWorkers, MaxWorkers, Repeats, Acc)
  when NumWorkers =< MaxWorkers ->
  io:format("currently ~p number of workers~n", [NumWorkers]),
  Time = repeat_times(distr, TimeType, Repeats, NumWorkers, MaxWorkers + 4),
  io:format("times for ~p runs in this round is ~p~n", [Repeats, Time]),
  time_against_workers(TimeType, NumWorkers + 2, MaxWorkers, Repeats, [Time | Acc]).


-spec write_results(list(), integer()) -> ok.
write_results(Res, Repeats) ->
  {ok, Stream} = file:open(?res_path, [append]),
  io:format(Stream,
            "results of timing measurement: {~p*times, #workers} ~n~p~n",
            [Repeats, Res]),
  file:close(Stream).

-spec repeat_times(Mode, TimeType, integer(), integer(), integer()) -> [integer()]
  when Mode :: distr | single_node,
       TimeType :: cpu | wallclock.
repeat_times(distr, TimeType, Repeats, NumWorkers, NumTasks) ->
  [time_distr_nodes(TimeType, NumWorkers, NumTasks) || _ <- lists:seq(1, Repeats)].


repeat_times(single, _TimeType, Repeats) ->
  Res = [time_single_node() || _ <- lists:seq(1, Repeats)],
  {ok, Stream} = file:open(?res_path, [append]),
  io:format(Stream, "single node evaluation repeating ~p*times~n", [Repeats]),
  io:format(Stream, "results of timing measurement ~p~n", [Res]),
  file:close(Stream).

-spec time_single_node() -> integer().
time_single_node() ->
  QryName = preproc:get_output_name(file, ?PROG),
  {Time, _R} = timer:tc(erlog_worker, run_program, [single, ?PROG, QryName]),
  io:format("time used in millisecond is ~p~n", [Time / 1000]),
  Time.

-spec time_distr_nodes(TimeType, integer(), integer()) -> integer()
  when TimeType :: wallclock | cpu.
time_distr_nodes(wallclock, NumWorkers, NumTasks) ->
  Cfg = erlog_worker:distr_setup(?PROG, NumWorkers, NumTasks, ?tmp_path),
  io:format("set up complete~n"),
  QryName = preproc:get_output_name(file, ?PROG),
  {Time, _R} = timer:tc(erlog_worker, distr_run, [Cfg, QryName, ?tmp_path]),
  io:format("ready to clean up~n"),
  erlog_worker:distr_clean(Cfg),
  io:format("time used in millisecond is ~p~n", [Time / 1000]),
  Time;
time_distr_nodes(cpu, NumWorkers, NumTasks) ->
  Cfg = erlog_worker:distr_setup(?PROG, NumWorkers, NumTasks, ?tmp_path),
  io:format("set up complete~n"),
  S1 = statistics(runtime),
  io:format("stats 1 ~p~n", [S1]),
  QryName = preproc:get_output_name(file, ?PROG),
  erlog_worker:distr_run(Cfg, QryName, ?tmp_path),
  {_TotTime, TimeSince} = statistics(runtime),
  io:format("ready to clean up~n"),
  erlog_worker:distr_clean(Cfg),
  io:format("time used in millisecond is ~p~n", [TimeSince]),
  TimeSince.
