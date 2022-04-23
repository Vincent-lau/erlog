-module(time_docker).

-compile(export_all).
-compile(nowarn_export_all).


coor_node() ->
  'coor@host.com'.

worker_nodes() ->
  lists:filter(fun (Node) ->
      case string:slice(hd(string:tokens(atom_to_list(Node), "@")), 0, 4) of
        "work" -> true;
        _Other -> false
      end
   end, nodes()).


start() ->
  R1 = erpc:multicall(worker_nodes(), worker, start_working, []),
  lager:info("worker start_working rpc results ~p~n", [R1]),
  Time = timer:tc(coordinator, wait_for_finish, [1000 * 60]),
  lager:notice("time is ~p~n", [Time]).


reset() ->
  supervisor:terminate_child(coor_sup, main),
  R1 = erpc:multicall(worker_nodes(), supervisor, terminate_child, [worker_sup, main]),
  supervisor:restart_child(coor_sup, main),
  R2 = erpc:multicall(worker_nodes(), supervisor, restart_child, [worker_sup, main]),
  lager:notice("results of reset rpc calls ~p", [R1 ++ R2]).
