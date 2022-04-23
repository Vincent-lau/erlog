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
  io:format("worker start_working rpc results ~p~n", [R1]),
  Time = timer:tc(coordinator, wait_for_finish, [1000 * 60]),
  io:format("time is ~p~n", [Time]).
