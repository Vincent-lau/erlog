-module(bench).

-export([main/1]).

main(_Args) ->
  application:start(erlog),
  lists:foreach(fun (P) -> timing:start({failure, P * 0.1}) end, lists:seq(2, 9)),
  application:stop(erlog).

usage() ->
  io:format("usage: bench mode percent\n"),
  halt(1).
