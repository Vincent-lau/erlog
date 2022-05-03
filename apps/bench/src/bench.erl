-module(bench).

-export([main/1]).

-define(res_path, "apps/bench/results/timing_res.txt").

main(_Args) ->
  no_fault().

straggle() ->
  {ok, Stream} = file:open(?res_path, [append]),
  io:format(Stream, "----------new experiment-----------~n", []),
  io:format(Stream, "~p~n", [calendar:local_time()]),
  lists:foreach(fun(P) ->
                   Cmd = io_lib:format("./apps/bench/bench_program/gen_and_run.sh ~p", [150]),
                   io:format("results of generating graph ~s~n", [os:cmd(Cmd)]),
                   application:start(erlog),
                   application:ensure_all_started([erlog]),
                   timing:start({straggle, P * 0.1}),
                   application:stop(erlog)
                end,
                [5]),
  io:format(Stream, "---------end of experiment---------~n", []),
  file:close(Stream).

no_fault() ->
  {ok, Stream} = file:open(?res_path, [append]),
  io:format(Stream, "~n~n----------new experiment-----------~n", []),
  io:format(Stream, "experiment start time ~p~n", [calendar:local_time()]),
  lists:foreach(fun(Size) ->
                   Cmd = io_lib:format("./apps/bench/bench_program/gen_and_run.sh ~p", [Size]),
                   io:format("results of generating graph ~s~n", [os:cmd(Cmd)]),
                   application:start(erlog),
                   timing:start(success),
                   application:stop(erlog)
                end,
                [10]),
  io:format(Stream, "experiment end time ~p~n", [calendar:local_time()]),
  io:format(Stream, "---------end of experiment---------~n", []),
  file:close(Stream).

usage() ->
  io:format("usage: bench mode percent\n"),
  halt(1).
