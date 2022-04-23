-module(erlog_srv).

-export([start_link/0, init/1, start_link/1]).

start_link(ProgName) ->
  gen_server:start_link(?MODULE, [ProgName], []).

start_link() ->
  gen_server:start(?MODULE, [], []).

init([ProgName]) ->
  %% Here we ignore what OTP asks of us and just do
  %% however we please.
  process_flag(trap_exit, true),
  ets:new(dl_atom_names, [named_table, public]),
  lager:debug("Application Erlog started!"),
  Name = hd(string:tokens(atom_to_list(node()), "@")),
  case string:slice(Name, 0, 4) of
    "work" ->
      lager:info("started in worker mode"),
      worker_sup:start_link();
    "coor" ->
      lager:info("started in coordinator mode"),
      coor_sup:start_link(ProgName);
    "nono" ->
      lager:info("no node name given, do nothing", [node()]);
    _Other ->
      lager:info("some strange node name ~p", [node()])
  end,
  {ok, []};
init([]) ->
  ets:new(dl_atom_names, [named_table, public]),
  lager:debug("Application Erlog started!"),
  lager:info("no program name given, do nothing"),
  {ok, []}.

terminate(shutdown, _State) ->
  ets:delete(dl_atom_names),
  ok.
