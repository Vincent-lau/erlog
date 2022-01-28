-module(dconfig).

-compile(export_all).

-record(node_config, {nodes :: #{node() => port()}}).

-type config() :: #node_config{}.

-spec get_node_host(node()) -> string().
get_node_host(Node) ->
  element(2, get_node_name_host(Node)).

-spec get_node_name(node()) -> string().
get_node_name(Node) ->
  element(1, get_node_name_host(Node)).

-spec get_node_name_host(node()) -> {string(), string()}.
get_node_name_host(Node) ->
  [Name, Host] = string:tokens(atom_to_list(Node), "@"),
  {Name, Host}.

-spec is_shortname(node()) -> boolean().
is_shortname(NodeName) ->
  Host = get_node_host(NodeName),
  case string:find(Host, ".") of
    nomatch ->
      true;
    _S ->
      false
  end.

%%----------------------------------------------------------------------
%% @doc
%% @param Name is the name of the node
%% <p>
%% This function will check whether the input Name is a short or long name,
%% and return the string cmd to run.
%% </p>
%%
%% <p>
%% For example, if the name is node1@127.0.0.1, which means this is a
%% fully qualified domain name, so this function will return
%% "-name node1@127.0.0.1"
%% </p>
%% @end
%%----------------------------------------------------------------------
-spec get_name_cmd(node()) -> string().
get_name_cmd(NodeName) ->
  case is_shortname(NodeName) of
    true ->
      "-sname " ++ atom_to_list(NodeName);
    false ->
      "-name " ++ atom_to_list(NodeName)
  end.

%%----------------------------------------------------------------------
%% @doc
%% @param PA is the path to the executable which can be empty.
%% @end
%%----------------------------------------------------------------------
-spec start_node(atom(), string()) -> port().
start_node(Name, PA) ->
  NameCmd = get_name_cmd(Name),
  Cmd = io_lib:format("erl -noshell -noinput ~s -pa ~s", [NameCmd, PA]),
  erlang:open_port({spawn, Cmd}, []).

-spec stop_node(node(), config()) -> true.
stop_node(NodeName, #node_config{nodes = Nodes}) ->
  try
    erpc:call(NodeName, init, stop, []),
    Port = maps:get(NodeName, Nodes),
    true = erlang:port_close(Port)
  catch
    error:{erpc, noconnection} ->
      io:format("Node might have already died skip it ~n")
  end.

-spec get_nodes(config()) -> [node()].
get_nodes(#node_config{nodes = Nodes}) ->
  maps:keys(Nodes).

-spec get_num_nodes(config()) -> non_neg_integer().
get_num_nodes(#node_config{nodes = Nodes}) ->
  maps:size(Nodes).

all_start(Cfg) ->
  multicall(worker, start, [], Cfg).

%%----------------------------------------------------------------------
%% @doc
%% Pick M numbers from 1...N
%% @end
%%----------------------------------------------------------------------
-spec pick_n(integer(), integer()) -> sets:set().
pick_n(M, N) when M =< N ->
  pick_n(M, N, sets:new()).

-spec pick_n(integer(), integer(), sets:set()) -> sets:set().
pick_n(M, N, Acc) ->
  case sets:size(Acc) == M of
    true ->
      Acc;
    false ->
      R = rand:uniform(N) - 1,
      pick_n(M, N, sets:add_element(R, Acc))
  end.

%%----------------------------------------------------------------------
%% @doc
%% Given the number of workers that should fail, randomly choose that
%% number of workers to fail
%% @end
%%----------------------------------------------------------------------
-spec fail_start(config(), integer()) -> ok.
fail_start(Cfg, FailNum) ->
  FailIndices = pick_n(FailNum, get_num_nodes(Cfg)),
  Nodes = get_nodes(Cfg),
  listsi:mapi(fun(Node, Idx) ->
                 case sets:is_element(Idx, FailIndices) of
                   true -> call(Node, worker, start, [failure]);
                   false -> call(Node, worker, start, [success])
                 end
              end,
              Nodes).

all_work(Cfg) ->
  multicall(worker, start_working, [], Cfg).

multicall(Module, Function, Args, Cfg) ->
  Nodes = get_nodes(Cfg),
  erpc:multicall(Nodes, Module, Function, Args).

call(Node, Module, Function, Args) ->
  erpc:call(Node, Module, Function, Args).

start_cluster([BaseName], Num) ->
  start_cluster([BaseName], Num, os:cmd("rebar3 path")).

-spec start_cluster(list(), integer(), string()) -> config().
start_cluster([BaseName], Num, PA) ->
  case string:find(atom_to_list(BaseName), "@") of
    nomatch ->
      NodeName = list_to_atom(atom_to_list(BaseName) ++ "@127.0.0.1"),
      start_cluster([NodeName, longnames], Num, PA);
    _S ->
      start_cluster([BaseName, longnames], Num, PA)
  end;
start_cluster([BaseName, longnames], Num, PA) ->
  {Name, Host} = get_node_name_host(BaseName),
  NodeNames =
    [list_to_atom(Name ++ integer_to_list(N) ++ "@" ++ Host) || N <- lists:seq(1, Num)],
  NodePids = lists:map(fun(N) -> start_node(N, PA) end, NodeNames),
  wait_for_start(NodeNames),
  Nodes = lists:zip(NodeNames, NodePids),
  #node_config{nodes = maps:from_list(Nodes)}.

wait_for_start(NodeNames) ->
  wait_for_nodes(NodeNames, pong, 1, 10).

wait_for_stop(NodeNames) ->
  wait_for_nodes(NodeNames, pang, 1, 10).

-spec wait_for_nodes([node()], pong | pang, integer(), integer()) -> ok.
wait_for_nodes(_N, _WT, N, MaxTry) when N >= MaxTry ->
  exit(waited_too_long);
wait_for_nodes(NodeNames, WaitType, N, MaxTry) when N < MaxTry ->
  timer:sleep(1000),
  PingRes = lists:map(fun(Node) -> net_adm:ping(Node) end, NodeNames),
  case lists:all(fun(R) -> R =:= WaitType end, PingRes) of
    true ->
      ok;
    false ->
      wait_for_nodes(NodeNames, WaitType, N + 1, MaxTry)
  end.

-spec stop_cluster(config()) -> StopRes when StopRes :: list().
stop_cluster(Cfg) ->
  R = lists:map(fun(NodeName) -> stop_node(NodeName, Cfg) end, get_nodes(Cfg)),
  wait_for_stop(get_nodes(Cfg)),
  R.

-spec add_node(atom(), config()) -> config().
add_node(Name, Cfg) ->
  ok.

%%----------------------------------------------------------------------
%% @doc
%% This function will remove the node from the cluster if it is present,
%% and cleanup all the resources that the node has used.
%% @end
%%----------------------------------------------------------------------
-spec remove_node(node(), config()) -> config().
remove_node(Name, Cfg) ->
  ok.

-spec isolate([node()], atom()) -> ok.
isolate(Nodes, Id) ->
  not_impl.

-spec isolate_end([node()]) -> ok.
isolate_end(Nodes) ->
  not_impl.
