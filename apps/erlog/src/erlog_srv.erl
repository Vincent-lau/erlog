-module(erlog_srv).

-export([start_link/0, init/1]).

start_link() ->
  gen_server:start_link(?MODULE, [], []).

init([]) ->
  %% Here we ignore what OTP asks of us and just do
  %% however we please.
  process_flag(trap_exit, true),
  % ets:new(table, [named_table, public]),
  io:format("Hello, heavy world!~n"),
  {ok, []}.

terminate(shutdown, State) ->
  io:format("terminate called~n"),
  % ets:delete(table),
  ok.
