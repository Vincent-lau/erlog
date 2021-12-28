-module(erlog_srv).

-export([start_link/0, init/1]).

start_link() ->
  gen_server:start_link(?MODULE, [], []).

init([]) ->
  %% Here we ignore what OTP asks of us and just do
  %% however we please.
  process_flag(trap_exit, true),
  ets:new(dl_atom_names, [named_table, public]),
  case filelib:is_dir("log") of
    true ->
      file:delete("log/erlang.log");
    false ->
      ok
    end,
  io:format("Hello, heavy world!~n"),

  {ok, []}.

terminate(shutdown, _State) ->
  io:format("terminate called~n"),
  ets:delete(dl_atom_names),
  ok.
