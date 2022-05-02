-module(erlog_srv).

-export([start_link/0, init/1, start_link/1]).

-export([start_working/0, reset_worker/0]).

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
      lager:info("no node name given, do nothing");
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

worker_nodes() ->
  lists:filter(fun (Node) ->
      case string:slice(hd(string:tokens(atom_to_list(Node), "@")), 0, 4) of
        "work" -> true;
        _Other -> false
      end
   end, nodes()).


start_working() ->
  R1 = erpc:multicall(worker_nodes(), worker, start_working, []),
  lager:info("worker start_working rpc results ~p~n", [R1]),
  Time = timer:tc(coordinator, wait_for_finish, [1000 * 60]),
  lager:info("time is ~p~n", [Time]).


reset_worker() ->
  supervisor:terminate_child(coor_sup, main),
  R1 = erpc:multicall(worker_nodes(), supervisor, terminate_child, [worker_sup, main]),
  supervisor:restart_child(coor_sup, main),
  R2 = erpc:multicall(worker_nodes(), supervisor, restart_child, [worker_sup, main]),
  lager:notice("results of reset rpc calls ~p", [R1 ++ R2]).

