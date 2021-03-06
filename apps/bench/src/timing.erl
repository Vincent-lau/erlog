-module(timing).

-export([start/0, start/1]).

-define(PROG, "apps/bench/bench_program/tc_bench.dl").
-define(tmp_path, "apps/bench/tmp/").
-define(res_path, "apps/bench/results/timing_res.txt").

-include_lib("kernel/include/logger.hrl").

start() ->
  start(success).

start(WorkerSpec) ->
  lager:info("current timing program ~p~n", [?PROG]),
  time_against_workers(wallclock, WorkerSpec, 5, 8, 2).

% start(single) ->
%   repeat_times(single, foo, 3).

-spec time_against_workers(TimeType, integer()) -> list()
  when TimeType :: cpu | wallclock.
time_against_workers(TimeType, MaxWorkers) ->
  time_against_workers(TimeType, success, 1, MaxWorkers, 10).


time_against_workers(TimeType, WSpec, MinWorkers, MaxWorkers, Repeats) ->
  time_against_workers(TimeType, WSpec, MinWorkers, MinWorkers, MaxWorkers, Repeats, []).

%%----------------------------------------------------------------------
%% @doc
%% This function will measure the time taken for a particular number of workers
%% to compute a particular program.
%%
%% @param Acc number of trials
%% @param MinWorkers starting number of workers
%%
%% @returns a list of {[time1, time2, ...], #workers}
%% @end
%%----------------------------------------------------------------------
-spec time_against_workers(TimeType,
                           worker:worker_spec(),
                           integer(),
                           integer(),
                           integer(),
                           integer(),
                           list()) ->
                            list()
  when TimeType :: cpu | wallclock.
time_against_workers(_TimeType, WSpec, NumWorkers, MinWorkers, MaxWorkers, Repeats, Acc)
  when NumWorkers > MaxWorkers ->
  Res =
    lists:zip(
      lists:reverse(Acc), lists:seq(MinWorkers, MaxWorkers, 2)),
  write_results(Res, WSpec, Repeats),
  Res;
time_against_workers(TimeType, WorkerSpec, NumWorkers, MinWorkers, MaxWorkers, Repeats, Acc)
  when NumWorkers =< MaxWorkers ->
  lager:info("currently ~p number of workers~n", [NumWorkers]),
  Time =
    repeat_times(distr, TimeType, WorkerSpec, Repeats, NumWorkers, trunc(MaxWorkers * 1.5)),
  lager:notice("times for ~p runs in this round is ~p~n", [Repeats, Time]),
  time_against_workers(TimeType,
                       WorkerSpec,
                       NumWorkers + 2,
                       MinWorkers,
                       MaxWorkers,
                       Repeats,
                       [Time | Acc]).

-spec write_results(list(), worker:worker_spec(), integer()) -> ok.
write_results(Res, WSpec, Repeats) ->
  {ok, Stream} = file:open(?res_path, [append]),
  io:format(Stream, "worker spec, ~p~n", [WSpec]),
  io:format(Stream,
            "results of timing measurement: {~p*times, #workers} ~n~p~n",
            [Repeats, Res]),
  io:format(Stream, "~n", []),
  file:close(Stream).

repeat_times(distr, TimeType, Repeats, NumWorkers, NumTasks) ->
  repeat_times(distr, TimeType, success, Repeats, NumWorkers, NumTasks).

-spec repeat_times(Mode, TimeType, WorkerSpec, integer(), integer(), integer()) ->
                    [integer()]
  when Mode :: distr | single_node,
       TimeType :: cpu | wallclock,
       WorkerSpec :: worker:worker_spec().
repeat_times(distr, TimeType, WorkerSpec, Repeats, NumWorkers, NumTasks) ->
  [time_distr_nodes(TimeType, WorkerSpec, NumWorkers, NumTasks)
   || _ <- lists:seq(1, Repeats)].

repeat_times(single, _TimeType, Repeats) ->
  Res = [time_single_node() || _ <- lists:seq(1, Repeats)],
  {ok, Stream} = file:open(?res_path, [append]),
  io:format(Stream, "single node evaluation repeating ~p*times~n", [Repeats]),
  io:format(Stream, "results of timing measurement ~p~n", [Res]),
  file:close(Stream).

-spec time_single_node() -> integer().
time_single_node() ->
  QryName = preproc:get_output_name(file, ?PROG),
  {Time, _R} = timer:tc(erlog_runner, run_program, [single, ?PROG, QryName]),
  lager:info("time used in millisecond is ~p~n", [Time / 1000]),
  Time.

-spec time_distr_nodes(TimeType, integer(), integer()) -> integer()
  when TimeType :: wallclock | cpu.
time_distr_nodes(TimeType, NumWorkers, NumTasks) ->
  time_distr_nodes(TimeType, success, NumWorkers, NumTasks).

-spec time_distr_nodes(TimeType, worker:worker_spec(), integer(), integer()) -> integer()
  when TimeType :: wallclock | cpu.
time_distr_nodes(wallclock, WorkerSpec, NumWorkers, NumTasks) ->
  Cfg = erlog_runner:distr_setup(?PROG, NumWorkers, NumTasks, ?tmp_path, WorkerSpec),
  lager:info("set up complete"),
  QryName = preproc:get_output_name(file, ?PROG),
  {Time, _R} = timer:tc(erlog_runner, distr_run, [Cfg, QryName, ?tmp_path]),
  ?LOG_DEBUG("ready to clean up~n"),
  erlog_runner:distr_clean(Cfg),
  lager:info("time used in millisecond is ~p~n", [Time / 1000]),
  Time;
time_distr_nodes(cpu, WorkerSpec, NumWorkers, NumTasks) ->
  Cfg = erlog_runner:distr_setup(?PROG, NumWorkers, NumTasks, ?tmp_path, WorkerSpec),
  lager:info("set up complete"),
  S1 = statistics(runtime),
  lager:info("stats 1 ~p~n", [S1]),
  QryName = preproc:get_output_name(file, ?PROG),
  erlog_runner:distr_run(Cfg, QryName, ?tmp_path),
  {_TotTime, TimeSince} = statistics(runtime),
  ?LOG_DEBUG("ready to clean up~n"),
  erlog_runner:distr_clean(Cfg),
  lager:info("time used in millisecond is ~p~n", [TimeSince]),
  TimeSince.
